import os
import json
import requests
from datetime import datetime, date
from supabase import create_client

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
TABLE = "raw_trades_ingest"

def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v

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

def main():
    print("===== START =====")

    service_key = must_env("SERVICE_KEY")
    supabase_url = must_env("SUPABASE_URL")
    supabase_key = must_env("SUPABASE_SERVICE_ROLE_KEY")

    # 기본값: 오늘(한국 기준)
    today = date.today().isoformat()
    date_str = os.getenv("TARGET_DATE", today).strip() or today
    print("DATE:", date_str)

    # 일단 네가 성공했던 값(예시)로 시작
    markets = [
        {"whsl_mrkt_cd": "110001", "corp_cd": "11000103"},  # 예시
    ]

    client = create_client(supabase_url, supabase_key)

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
                "payload": it,  # 원본 통째 저장
            })

        resp = client.table(TABLE).insert(rows).execute()

        # supabase-py는 성공/실패를 resp에 담아줌 (실패 시 여기가 힌트)
        data = getattr(resp, "data", None)
        err = getattr(resp, "error", None)

        print("INSERT data len:", 0 if data is None else len(data))
        print("INSERT error:", err)

        if err:
            raise RuntimeError(f"Supabase insert 실패: {err}")

        total_inserted += (0 if data is None else len(data))

    print("\nTOTAL INSERTED:", total_inserted)
    print("===== DONE =====")

    # 0이면 실패로 처리해서 Actions에서 바로 눈에 띄게
    if total_inserted == 0:
        raise RuntimeError("INSERT가 0건임 (조건/테이블/키 확인 필요)")

if __name__ == "__main__":
    main()
