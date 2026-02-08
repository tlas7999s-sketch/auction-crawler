import os
import requests
from datetime import date
from supabase import create_client

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

RAW_TABLE = "raw_trades_ingest"
PUBLIC_TABLE = "tomato_trades_public"
MARKETS_TABLE = "markets"

def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v

def to_num(x):
    try:
        if x is None or x == "":
            return None
        return float(str(x))
    except:
        return None

def fetch_items(service_key: str, date_str: str, whsl_mrkt_cd: str, corp_cd: str,
                gds_lclsf_cd: str="08", gds_mclsf_cd: str="03", num_rows: int=50):
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
    resp = client.table(MARKETS_TABLE).select("whsl_mrkt_cd,corp_cd").execute()
    rows = resp.data or []
    return [{"whsl_mrkt_cd": str(r["whsl_mrkt_cd"]), "corp_cd": str(r["corp_cd"])} for r in rows]

def upsert_public_rows(client, date_str: str, items: list):
    rows = []
    for it in items:
        rows.append({
            "trd_clcln_ymd": date_str,
            "whsl_mrkt_cd": str(it.get("whsl_mrkt_cd", "")).strip(),
            "corp_cd": str(it.get("corp_cd", "")).strip(),

            "whsl_mrkt_nm": it.get("whsl_mrkt_nm"),
            "corp_nm": it.get("corp_nm"),

            "gds_lclsf_cd": it.get("gds_lclsf_cd"),
            "gds_mclsf_cd": it.get("gds_mclsf_cd"),
            "gds_sclsf_cd": it.get("gds_sclsf_cd"),
            "gds_sclsf_nm": it.get("gds_sclsf_nm"),

            "plor_cd": it.get("plor_cd"),
            "plor_nm": it.get("plor_nm"),

            "unit_cd": it.get("unit_cd"),
            "unit_nm": it.get("unit_nm"),
            "unit_qty": to_num(it.get("unit_qty")),
            "qty": to_num(it.get("qty")),

            "scsbd_prc": to_num(it.get("scsbd_prc")),
            "trd_se": it.get("trd_se"),
            "scsbd_dt": it.get("scsbd_dt"),

            "spm_no": str(it.get("spm_no", "")).strip() or None,
            "auctn_seq": str(it.get("auctn_seq", "")).strip() or None,
        })

    # spm_no unique 기준 upsert
    if rows:
        client.table(PUBLIC_TABLE).upsert(rows, on_conflict="spm_no").execute()
        print("UPSERT public rows:", len(rows))

def insert_raw_rows(client, date_str: str, whsl_mrkt_cd: str, corp_cd: str, items: list):
    if not items:
        return 0

    rows = []
    for it in items:
        rows.append({
            "trd_clcln_ymd": date_str,
            "whsl_mrkt_cd": whsl_mrkt_cd,
            "corp_cd": corp_cd,
            "gds_lclsf_cd": "08",
            "gds_mclsf_cd": "03",
            "payload": it,
        })

    resp = client.table(RAW_TABLE).insert(rows).execute()
    err = getattr(resp, "error", None)
    if err:
        raise RuntimeError(f"Supabase raw insert 실패: {err}")

    data = getattr(resp, "data", None)
    return 0 if data is None else len(data)

def main():
    print("===== START =====")

    service_key = must_env("SERVICE_KEY")
    supabase_url = must_env("SUPABASE_URL")
    supabase_key = must_env("SUPABASE_SERVICE_ROLE_KEY")

    client = create_client(supabase_url, supabase_key)

    today = date.today().isoformat()
    date_str = os.getenv("TARGET_DATE", today).strip() or today
    print("DATE:", date_str)

    markets = load_markets(client)
    if not markets:
        raise RuntimeError("markets 테이블이 비어있음. 먼저 markets를 채워야 함!")

    total_raw = 0
    total_public = 0

    for m in markets:
        whsl_mrkt_cd = m["whsl_mrkt_cd"]
        corp_cd = m["corp_cd"]
        print(f"\n--- MARKET {whsl_mrkt_cd} / CORP {corp_cd} ---")

        items = fetch_items(service_key, date_str, whsl_mrkt_cd, corp_cd)
        print("ITEMS:", len(items))

        # raw 저장
        inserted = insert_raw_rows(client, date_str, whsl_mrkt_cd, corp_cd, items)
        total_raw += inserted

        # public upsert(앱용)
        if items:
            upsert_public_rows(client, date_str, items)
            total_public += len(items)

    print("\nTOTAL RAW INSERTED:", total_raw)
    print("TOTAL PUBLIC UPSERTED:", total_public)
    print("===== DONE =====")

if __name__ == "__main__":
    main()
