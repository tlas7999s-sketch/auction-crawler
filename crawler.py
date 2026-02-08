import os
import requests
from datetime import date
from supabase import create_client

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
INGEST_TABLE = "raw_trades_ingest"   # 네 Supabase 테이블명
MARKETS_TABLE = "markets"            # 네 Supabase markets 테이블명


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v


def fetch_items(service_key: str, date_str: str, whsl_mrkt_cd: str, corp_cd: str,
                gds_lclsf_cd: str = "08", gds_mclsf_cd: str = "03", num_rows: int = 50):
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

    print("REQUEST URL:", res.url.replace(service_key, "***"))
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
    return items


def load_markets(client):
    # markets 테이블에서 (whsl_mrkt_cd, corp_cd) 가져오기
    resp = client.table(MARKETS_TABLE).select("whsl_mrkt_cd,corp_cd").execute()
    rows = resp.data or []
    return [{"whsl_mrkt_cd": str(r["whsl_mrkt_cd"]), "corp_cd": str(r["corp_cd"])} for r in rows]


def upsert_market_info(client, items):
    rows = []
    for it in items:
        wcd = str(it.get("whsl_mrkt_cd", "")).strip()
        ccd = str(it.get("corp_cd", "")).strip()
        if not wcd or not ccd:
            continue
        rows.append({
            "whsl_mrkt_cd": wcd,
            "corp_cd": ccd,
            "whsl_mrkt_nm": it.get("whsl_mrkt_nm"),
            "corp_nm": it.get("corp_nm"),
        })

    if rows:
        client.table(MARKETS_TABLE).upsert(rows, on_conflict="whsl_mrkt_cd,corp_cd").execute()
        print("UPSERT markets:", len(rows))


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

    markets = load_markets(client)
    if not markets:
        raise RuntimeError("markets 테이블이 비어있음. 먼저 markets를 채워야 함!")

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

        # markets 이름 업데이트(옵션)
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
                "payload": it,  # 원본 통째 저장(JSON)
            })

        resp = client.table(INGEST_TABLE).insert(rows).execute()
        data = getattr(resp, "data", None)
        err = getattr(resp, "error", None)

        print("INSERT data len:", 0 if data is None else len(data))
        print("INSERT error:", err)

        if err:
            raise RuntimeError(f"Supabase insert 실패: {err}")

        total_inserted += (0 if data is None else len(data))

    print("\nTOTAL INSERTED:", total_inserted)
    print("===== DONE =====")

    # 0건이어도 정상 종료(데이터 없는 날도 있어서)
    return


if __name__ == "__main__":
    main()
