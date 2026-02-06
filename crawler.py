import os
import requests
from datetime import date
from supabase import create_client

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def supa():
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)

def fetch_data(date_str: str, whsl_mrkt_cd: str, corp_cd: str):
    service_key = os.getenv("SERVICE_KEY", "").strip()
    if not service_key:
        raise RuntimeError("SERVICE_KEY 비어있음")

    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 50,
        "returnType": "json",
        "cond[whsl_mrkt_cd::EQ]": whsl_mrkt_cd,
        "cond[corp_cd::EQ]": corp_cd,
        "cond[gds_lclsf_cd::EQ]": "08",  # 과일과채류 (예시)
        "cond[gds_mclsf_cd::EQ]": "03",  # 토마토 (예시)
        "cond[trd_clcln_ymd::EQ]": date_str,
    }

    res = requests.get(BASE_URL, params=params, timeout=30)
    print("HTTP:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))
    print("Body(head 200):", res.text[:200])
    res.raise_for_status()
    return res.json()

def to_float(x):
    try:
        return float(x)
    except:
        return None

def save_raw(items, meta):
    client = supa()

    rows = []
    for it in items:
        rows.append({
            "trd_clcln_ymd": meta["date"],
            "whsl_mrkt_cd": it.get("whsl_mrkt_cd"),
            "whsl_mrkt_nm": it.get("whsl_mrkt_nm"),
            "corp_cd": it.get("corp_cd"),
            "corp_nm": it.get("corp_nm"),
            "gds_lclsf_cd": it.get("gds_lclsf_cd"),
            "gds_lclsf_nm": it.get("gds_lclsf_nm"),
            "gds_mclsf_cd": it.get("gds_mclsf_cd"),
            "gds_mclsf_nm": it.get("gds_mclsf_nm"),
            "gds_sclsf_cd": it.get("gds_sclsf_cd"),
            "gds_sclsf_nm": it.get("gds_sclsf_nm"),
            "gds_sclsfc_nm": it.get("gds_sclsfc_nm"),
            "gds_id": it.get("gds_id"),
            "gds_nm": it.get("gds_nm"),
            "unit_nm": it.get("unit_nm"),
            "unit_qty": to_float(it.get("unit_qty")),
            "qty": to_float(it.get("qty")),
            "amt": to_float(it.get("amt")),
            "kg_amt": to_float(it.get("kg_amt")),
            "payload": it,
        })

    if rows:
        client.table("auction_trades_raw").insert(rows).execute()
        print("RAW inserted:", len(rows))
    else:
        print("No items -> skip insert")

def save_daily_agg(items, date_str):
    # 그래프용: 시장/단위별 평균(kg_amt) 집계 저장
    # (여기선 “토마토 + 2.5kg”만 예시로 집계)
    client = supa()

    bucket = {}
    for it in items:
        unit_qty = to_float(it.get("unit_qty"))
        gname = it.get("gds_mclsf_nm")
        if gname != "토마토":
            continue
        if unit_qty != 2.5:
            continue

        key = (it.get("whsl_mrkt_cd"), it.get("whsl_mrkt_nm"), gname, unit_qty)
        kg_amt = to_float(it.get("kg_amt"))
        if kg_amt is None:
            continue

        bucket.setdefault(key, []).append(kg_amt)

    upserts = []
    for (mcd, mnm, gname, unit_qty), vals in bucket.items():
        upserts.append({
            "trd_clcln_ymd": date_str,
            "whsl_mrkt_cd": mcd,
            "whsl_mrkt_nm": mnm,
            "gds_mclsf_nm": gname,
            "unit_qty": unit_qty,
            "avg_kg_amt": sum(vals)/len(vals),
            "min_kg_amt": min(vals),
            "max_kg_amt": max(vals),
            "count_items": len(vals),
        })

    if upserts:
        client.table("auction_daily_agg").upsert(upserts, on_conflict="trd_clcln_ymd,whsl_mrkt_cd,gds_mclsf_nm,unit_qty").execute()
        print("AGG upserted:", len(upserts))
    else:
        print("No agg rows")

def main():
    today = date.today().isoformat()

    # ✅ 일단 “성공했던 값” 1개로 테스트
    whsl_mrkt_cd = "110001"
    corp_cd = "11000103"

    data = fetch_data(today, whsl_mrkt_cd, corp_cd)
    body = data.get("response", {}).get("body", {})
    items = body.get("items", {}).get("item", [])
    if isinstance(items, dict):
        items = [items]

    print("ITEMS COUNT:", len(items))
    save_raw(items, {"date": today})
    save_daily_agg(items, today)

if __name__ == "__main__":
    main()
