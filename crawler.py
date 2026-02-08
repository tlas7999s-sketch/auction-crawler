import os
import time
import requests
from datetime import date
from supabase import create_client

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
RAW_TABLE = "raw_trades_ingest"

# 토마토(과일과채류/토마토)
GDS_LCLSF_CD = "08"
GDS_MCLSF_CD = "03"

def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v

def fetch_page(service_key: str, date_str: str, page_no: int, num_rows: int):
    # ✅ 전국 수집: whsl_mrkt_cd/corp_cd 조건을 넣지 않음
    params = {
        "serviceKey": service_key,
        "pageNo": page_no,
        "numOfRows": num_rows,
        "returnType": "json",
        "cond[gds_lclsf_cd::EQ]": GDS_LCLSF_CD,
        "cond[gds_mclsf_cd::EQ]": GDS_MCLSF_CD,
        "cond[trd_clcln_ymd::EQ]": date_str,
    }

    res = requests.get(BASE_URL, params=params, timeout=30)
    print(f"HTTP {res.status_code} / pageNo={page_no} numOfRows={num_rows}")
    res.raise_for_status()

    data = res.json()
    body = data.get("response", {}).get("body", {}) or {}
    items = (body.get("items", {}) or {}).get("item", []) or []
    if isinstance(items, dict):
        items = [items]
    total_count = body.get("totalCount")
    return items, total_count

def upsert_markets(client, items):
    """
    ✅ 핵심: raw 응답에서 발견된 시장/법인 코드+이름을 markets 테이블에 누적 저장
    """
    rows = []
    seen = set()
    for it in items:
        wcd = str(it.get("whsl_mrkt_cd", "")).strip()
        ccd = str(it.get("corp_cd", "")).strip()
        if not wcd or not ccd:
            continue
        key = (wcd, ccd)
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "whsl_mrkt_cd": wcd,
            "corp_cd": ccd,
            "whsl_mrkt_nm": it.get("whsl_mrkt_nm"),
            "corp_nm": it.get("corp_nm"),
        })

    if rows:
        client.table("markets").upsert(rows, on_conflict="whsl_mrkt_cd,corp_cd").execute()
        print("UPSERT markets:", len(rows))

def insert_raw(client, date_str: str, items):
    if not items:
        return 0

    rows = []
    for it in items:
        rows.append({
            "trd_clcln_ymd": date_str,
            "whsl_mrkt_cd": str(it.get("whsl_mrkt_cd", "")).strip() or None,
            "corp_cd": str(it.get("corp_cd", "")).strip() or None,
            "gds_lclsf_cd": GDS_LCLSF_CD,
            "gds_mclsf_cd": GDS_MCLSF_CD,
            "payload": it,
        })

    resp = client.table(RAW_TABLE).insert(rows).execute()
    err = getattr(resp, "error", None)
    if err:
        raise RuntimeError(f"Supabase insert 실패: {err}")
    data = getattr(resp, "data", None)
    inserted = 0 if data is None else len(data)
    print("INSERT raw:", inserted)
    return inserted

def main():
    print("===== START =====")
    service_key = must_env("SERVICE_KEY")
    supabase_url = must_env("SUPABASE_URL")
    supabase_key = must_env("SUPABASE_SERVICE_ROLE_KEY")
    client = create_client(supabase_url, supabase_key)

    today = date.today().isoformat()
    date_str = os.getenv("TARGET_DATE", today).strip() or today
    print("DATE:", date_str)

    page_no = 1
    num_rows = int(os.getenv("NUM_ROWS", "1000"))  # 200/500/1000 중에서 안정적인 값 사용
    total_inserted = 0

    while True:
        items, total_count = fetch_page(service_key, date_str, page_no, num_rows)
        print("ITEMS:", len(items), "totalCount:", total_count)

        if not items:
            break

        upsert_markets(client, items)
        total_inserted += insert_raw(client, date_str, items)

        page_no += 1
        time.sleep(0.2)

        if isinstance(total_count, int) and (page_no - 1) * num_rows >= total_count:
            break

    print("\nTOTAL INSERTED:", total_inserted)
    print("===== DONE =====")

if __name__ == "__main__":
    main()
