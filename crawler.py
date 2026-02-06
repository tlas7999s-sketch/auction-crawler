import os
import time
import requests
from datetime import date
from supabase import create_client

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

# ✅ 여기만 늘리면 "모든 시장" 확장 가능
# (whsl_mrkt_cd, corp_cd)
MARKETS = [
    ("110001", "11000103"),  # 너가 성공한 조합 (예시)
    # ("부산시장코드", "법인코드"),
]

def get_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v

def get_supabase():
    url = get_env("SUPABASE_URL")
    key = get_env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)

def fetch_data(date_str: str, whsl_mrkt_cd: str, corp_cd: str) -> dict:
    service_key = get_env("SERVICE_KEY")

    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 50,
        "returnType": "json",

        # ✅ 시장/법인
        "cond[whsl_mrkt_cd::EQ]": whsl_mrkt_cd,
        "cond[corp_cd::EQ]": corp_cd,

        # ✅ 품목 대분류/중분류 (너가 쓰던 값 유지)
        "cond[gds_lclsf_cd::EQ]": "08",
        "cond[gds_mclsf_cd::EQ]": "03",

        # ✅ 정산일자
        "cond[trd_clcln_ymd::EQ]": date_str,
    }

    res = requests.get(BASE_URL, params=params, timeout=30)

    # Actions 로그 확인용
    print("REQUEST URL:", res.url.replace(service_key, "***"))
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

def normalize_items(data: dict) -> list[dict]:
    body = (data.get("response") or {}).get("body") or {}
    items = (body.get("items") or {}).get("item") or []

    # item이 1개면 dict로 내려오는 경우 방지
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        items = []
    return items

def save_raw(items: list[dict], date_str: str):
    client = get_supabase()

    rows = []
    for it in items:
        rows.append({
            "trd_clcln_ymd": date_str,
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
            "payload": it,  # 원본 전체 저장
        })

    if not rows:
        print("RAW: no rows -> skip")
        return

    client.table("auction_trades_raw").insert(rows).execute()
    print("RAW inserted:", len(rows))

def save_daily_agg(items: list[dict], date_str: str):
    """
    그래프용 집계(옵션):
    - 토마토
    - unit_qty = 2.5
    - kg_amt 평균/최소/최대/건수
    """
    client = get_supabase()

    bucket = {}  # key -> list[kg_amt]
    meta = {}    # key -> (market_nm)

    for it in items:
        gname = it.get("gds_mclsf_nm")  # 예: "토마토"
        unit_qty = to_float(it.get("unit_qty"))
        kg_amt = to_float(it.get("kg_amt"))

        if gname != "토마토":
            continue
        if unit_qty != 2.5:
            continue
        if kg_amt is None:
            continue

        mcd = it.get("whsl_mrkt_cd")
        mnm = it.get("whsl_mrkt_nm")
        key = (mcd, gname, unit_qty)

        bucket.setdefault(key, []).append(kg_amt)
        meta[key] = mnm

    upserts = []
    for (mcd, gname, unit_qty), vals in bucket.items():
        upserts.append({
            "trd_clcln_ymd": date_str,
            "whsl_mrkt_cd": mcd,
            "whsl_mrkt_nm": meta.get((mcd, gname, unit_qty)),
            "gds_mclsf_nm": gname,
            "unit_qty": unit_qty,
            "avg_kg_amt": sum(vals) / len(vals),
            "min_kg_amt": min(vals),
            "max_kg_amt": max(vals),
            "count_items": len(vals),
        })

    if not upserts:
        print("AGG: no rows -> skip")
        return

    client.table("auction_daily_agg").upsert(
        upserts,
        on_conflict="trd_clcln_ymd,whsl_mrkt_cd,gds_mclsf_nm,unit_qty"
    ).execute()
    print("AGG upserted:", len(upserts))

def main():
    today = date.today().isoformat()
    print("===== START =====")
    print("DATE:", today)

    total_items = 0

    for whsl_mrkt_cd, corp_cd in MARKETS:
        print(f"\n--- MARKET {whsl_mrkt_cd} / CORP {corp_cd} ---")

        data = fetch_data(today, whsl_mrkt_cd, corp_cd)
        items = normalize_items(data)

        print("ITEMS COUNT:", len(items))
        if items:
            print("FIRST ITEM:", items[0])

        # ✅ Supabase 저장
        save_raw(items, today)
        save_daily_agg(items, today)

        total_items += len(items)

        # ✅ 무료/차단 방지: 시장 늘리면 약간 쉬기
        time.sleep(0.5)

    print("\nTOTAL ITEMS:", total_items)
    print("===== DONE =====")

if __name__ == "__main__":
    main()
