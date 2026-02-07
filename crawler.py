import os
from datetime import date

import requests
from supabase import create_client

from markets import MARKETS

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
TABLE = "raw_trades_ingest"


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v


def fetch_items(
    service_key: str,
    date_str: str,
    whsl_mrkt_cd: str,
    corp_cd: str,
    gds_lclsf_cd: str = "08",
    gds_mclsf_cd: str = "03",
    num_rows: int = 50,
):
    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": num_rows,
        "returnType": "json",
        "cond[whsl_mrkt_cd::EQ]": whsl_mrkt_cd,
        "cond[corp_cd::EQ]": corp_cd,
        "cond[gds_lclsf_cd::EQ]": gds_lclsf_cd,
        "cond[gds_mclsf_cd::EQ]": gds_mclsf_cd,
        "cond[trd_clcln_ymd::EQ]": date_str,
    }

    res = requests.get(BASE_URL, params=params, timeout=30)
    print("REQUEST URL:", res.url)
    print("HTTP:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))
    print("Body(head 200):", res.text[:200])

    res.raise_for_status()
    data = res.json()

    items = (
        data.get("response", {})
            .get("body", {})
            .get("items", {})
            .get("item", [])
    )

    if isinstance(items, dict):
        items = [items]
    if items is None:
        items = []

    return items


def load_markets_from_db(client):
    """
    Supabase markets 테이블에서 (whsl_mrkt_cd, corp_cd) 목록을 읽어온다.
    테이블이 없거나 권한/스키마 문제면 예외가 날 수 있으니 try/except로 감싼다.
    """
    try:
        resp = client.table("markets").select("whsl_mrkt_cd, corp_cd").execute()
        rows = resp.data or []
        markets = []
        for r in rows:
            w = str(r.get("whsl_mrkt_cd", "")).strip()
            c = str(r.get("corp_cd", "")).strip()
            if w and c:
                markets.append({"whsl_mrkt_cd": w, "corp_cd": c})
        return markets
    except Exception as e:
        print("WARN: markets 테이블 로드 실패(없을 수 있음). fallback to markets.py")
        print("DETAIL:", e)
        return []


def upsert_market_info(client, items):
    """
    응답 item 안에 whsl_mrkt_nm / corp_nm이 있으면 markets 테이블에 upsert로 업데이트한다.
    중복 제거해서 upsert.
    """
    uniq = {}
    for it in items:
        wcd = str(it.get("whsl_mrkt_cd", "")).strip()
        ccd = str(it.get("corp_cd", "")).strip()
        if not wcd or not ccd:
            continue

        key = (wcd, ccd)
        uniq[key] = {
            "whsl_mrkt_cd": wcd,
            "corp_cd": ccd,
            "whsl_mrkt_nm": it.get("whsl_mrkt_nm"),
            "corp_nm": it.get("corp_nm"),
        }

    rows = list(uniq.values())
    if not rows:
        return

    try:
        client.table("markets").upsert(rows, on_conflict="whsl_mrkt_cd,corp_cd").execute()
        print("UPSERT markets:", len(rows))
    except Exception as e:
        # markets 테이블이 없거나 RLS 때문에 실패할 수 있으니, 수집 자체는 계속 진행
        print("WARN: markets upsert 실패(수집은 계속).")
        print("DETAIL:", e)


def main():
    print("===== START =====")

    service_key = must_env("SERVICE_KEY")
    supabase_url = must_env("SUPABASE_URL")
    supabase_key = must_env("SUPABASE_SERVICE_ROLE_KEY")

    client = create_client(supabase_url, supabase_key)

    # 기본값: 오늘
    today = date.today().isoformat()
    date_str = os.getenv("TARGET_DATE", today).strip() or today
    print("DATE:", date_str)

    # 1) DB에 markets 테이블이 있으면 그걸 우선 사용
    markets = load_markets_from_db(client)

    # 2) DB가 비어있거나 실패하면 markets.py의 MARKETS를 사용
    if not markets:
        print("INFO: DB markets 비어있음 -> markets.py MARKETS 사용")
        markets = [{"whsl_mrkt_cd": w, "corp_cd": c} for (w, c) in MARKETS]

    if not markets:
        raise RuntimeError("시장 목록이 비어있음. markets 테이블 또는 markets.py를 채워야 함!")

    total_inserted = 0

    for m in markets:
        whsl_mrkt_cd = m["whsl_mrkt_cd"]
        corp_cd = m["corp_cd"]
        print(f"\n--- MARKET {whsl_mrkt_cd} / CORP {corp_cd} ---")

        items = fetch_items(
            service_key=service_key,
            date_str=date_str,
            whsl_mrkt_cd=whsl_mrkt_cd,
            corp_cd=corp_cd,
            gds_lclsf_cd="08",
            gds_mclsf_cd="03",
            num_rows=50,
        )

        print("ITEMS:", len(items))

        # markets 테이블 업데이트 시도(실패해도 수집은 계속)
        upsert_market_info(client, items)

        if not items:
            continue

        rows = []
        for it in items:
            rows.append({
                "trd_clcln_ymd": date_str,
                "whsl_mrkt_cd": whsl_mrkt_cd,
                "corp_cd": corp_cd,
                "gds_lclsf_cd": "08",
                "gds_mclsf_cd": "03",
                "payload": it,  # 원본 JSON 저장
            })

        resp = client.table(TABLE).insert(rows).execute()
        data = getattr(resp, "data", None)

        inserted = 0 if data is None else len(data)
        print("INSERTED:", inserted)

        total_inserted += inserted

    print("\nTOTAL INSERTED:", total_inserted)
    print("===== DONE =====")

    if total_inserted == 0:
        raise RuntimeError("INSERT가 0건임 (조건/테이블/키/날짜 확인 필요)")


if __name__ == "__main__":
    main()
