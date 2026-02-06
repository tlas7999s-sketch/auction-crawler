import os
import json
import time
import requests
from datetime import datetime, timedelta

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def fetch_page(service_key: str, trd_date: str, page_no: int, num_rows: int = 50):
    params = {
        "serviceKey": service_key,
        "pageNo": page_no,
        "numOfRows": num_rows,
        "returnType": "json",
        # 품목: 과일과채류(08) / 토마토(03)
        "cond[gds_lclsf_cd::EQ]": "08",
        "cond[gds_mclsf_cd::EQ]": "03",
        # 거래정산일자
        "cond[trd_clcln_ymd::EQ]": trd_date,
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_items(payload: dict):
    try:
        items = payload["response"]["body"]["items"]["item"]
        if isinstance(items, dict):
            return [items]
        return items or []
    except Exception:
        return []

def is_target(row: dict) -> bool:
    # 부산(산지) + 2.5kg + 토마토(중분류)
    plor = str(row.get("plor_nm", ""))
    unit_qty = str(row.get("unit_qty", ""))
    gds_m = str(row.get("gds_mclsf_cd", ""))

    return ("부산" in plor) and (unit_qty == "2.500") and (gds_m == "03")

def main():
    service_key = os.getenv("SERVICE_KEY", "").strip()
    if not service_key:
        raise RuntimeError("SERVICE_KEY env missing. Set GitHub Actions secret SERVICE_KEY.")

    # 오늘 기준(원하면 어제까지 같이 돌릴 수 있음)
    trd_date = datetime.now().strftime("%Y-%m-%d")

    all_target = []
    page = 1
    num_rows = 50

    while True:
        data = fetch_page(service_key, trd_date, page, num_rows=num_rows)
        body = data.get("response", {}).get("body", {})
        total = int(body.get("totalCount", 0) or 0)

        items = normalize_items(data)
        if not items:
            break

        for row in items:
            if is_target(row):
                all_target.append(row)

        # 페이지 종료 조건
        if page * num_rows >= total:
            break

        page += 1
        time.sleep(0.2)  # 과도호출 방지(무료/안정)

    # 결과 저장
    out = {
        "trd_date": trd_date,
        "count": len(all_target),
        "items": all_target,
    }

    print(f"[OK] trd_date={trd_date} target_count={len(all_target)}")
    # 콘솔에서 대략 확인
    for row in all_target[:10]:
        print(row.get("whsl_mrkt_nm"), row.get("corp_nm"), row.get("plor_nm"), row.get("unit_qty"), row.get("scsbd_prc"))

    # 파일 저장(액션 아티팩트/깃 커밋 등으로 확장 가능)
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
