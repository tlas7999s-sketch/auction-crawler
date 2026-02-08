import os
import math
import requests
from datetime import date
from supabase import create_client


BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"
TABLE = "raw_trades_ingest"


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} 가 비어있음. GitHub Secrets에 {name} 등록 확인!")
    return v


def fetch_page(service_key: str, date_str: str, whsl_mrkt_cd: str, corp_cd: str,
               gds_lclsf_cd: str, gds_mclsf_cd: str,
               page_no: int, num_rows: int):
    params = {
        "serviceKey": service_key,
        "pageNo": page_no,
        "numOfRows": num_rows,
        "returnType": "json",
        "cond[whsl_mrkt_cd::EQ]": whsl_mrkt_cd,
        "cond[corp_cd::EQ]": corp_cd,
        "cond[gds_lclsf_cd::EQ]": gds_lclsf_cd,
        "cond[gds_mclsf_cd::EQ]": gds_mclsf_cd,
        "cond[trd_clcln_ymd::EQ]": date_str,
    }

    res = requests.get(BASE_URL, params=params, timeout=30)
    res.raise_for_status()
    data = res.json()

    body = data.get("response", {}).get("body", {}) or {}
    total_count = int(body.get("totalCount") or 0)

    items = body.get("items", {}).get("item", [])
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        items = []

    return total_count, items, res.url


def load_markets(client):
    # markets 테이블에 whsl_mrkt_cd / corp_cd가 들어있다는 전제
    rows = client.table("markets").select("whsl_mrkt_cd, corp_cd").execute().data or []
    return [{"whsl_mrkt_cd": str(r["whsl_mrkt_cd"]), "corp_cd": str(r["corp_cd"])} for r in rows]


def make_row_key(date_str: str, whsl_mrkt_cd: str, corp_cd: str, it: dict) -> str:
    # 스샷/데이터에 spm_no가 매우 유용 (거의 유니크하게 동작)
    spm_no = str(it.get("spm_no", "")).strip()
    auctn_seq = str(it.get("auctn_seq", "")).strip()
    scsbd_dt = str(it.get("scsbd_dt", "")).strip()

    # spm_no 있으면 그걸 최우선
    if spm_no:
        return f"{date_str}|{whsl_mrkt_cd}|{corp_cd}|spm:{spm_no}"

    # 없으면 보조키
    return f"{date_str}|{whsl_mrkt_cd}|{corp_cd}|auct:{auctn_seq}|dt:{scsbd_dt}"


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

    gds_lclsf_cd = os.getenv("GDS_LCLSF_CD", "08").strip() or "08"  # 과일과채류
    gds_mclsf_cd = os.getenv("GDS_MCLSF_CD", "03").strip() or "03"  # 토마토
    num_rows = int(os.getenv("NUM_ROWS", "100"))  # 50보다 크게 (API 허용범위 내에서)
    print("ROWS PER PAGE:", num_rows)

    total_upserted = 0

    for m in markets:
        whsl_mrkt_cd = m["whsl_mrkt_cd"]
        corp_cd = m["corp_cd"]

        print(f"\n--- MARKET {whsl_mrkt_cd} / CORP {corp_cd} ---")

        # 1페이지 먼저 호출해서 totalCount 확보
        total_count, items, url = fetch_page(
            service_key, date_str, whsl_mrkt_cd, corp_cd,
            gds_lclsf_cd, gds_mclsf_cd,
            page_no=1, num_rows=num_rows
        )
        print("REQUEST:", url)
        print("TOTAL COUNT:", total_count)
        print("PAGE 1 ITEMS:", len(items))

        all_items = list(items)

        # totalCount 기반 페이지 반복
        if total_count > num_rows:
            total_pages = int(math.ceil(total_count / num_rows))
            for page in range(2, total_pages + 1):
                _, page_items, _ = fetch_page(
                    service_key, date_str, whsl_mrkt_cd, corp_cd,
                    gds_lclsf_cd, gds_mclsf_cd,
                    page_no=page, num_rows=num_rows
                )
                print(f"PAGE {page} ITEMS:", len(page_items))
                if not page_items:
                    break
                all_items.extend(page_items)

        print("ALL ITEMS:", len(all_items))

        if not all_items:
            continue

        rows = []
        for it in all_items:
            row_key = make_row_key(date_str, whsl_mrkt_cd, corp_cd, it)
            rows.append({
                "row_key": row_key,
                "trd_clcln_ymd": date_str,
                "whsl_mrkt_cd": whsl_mrkt_cd,
                "corp_cd": corp_cd,
                "gds_lclsf_cd": gds_lclsf_cd,
                "gds_mclsf_cd": gds_mclsf_cd,
                "payload": it,
            })

        # upsert로 중복 방지
        resp = client.table(TABLE).upsert(rows, on_conflict="row_key").execute()
        data = getattr(resp, "data", None)
        err = getattr(resp, "error", None)

        if err:
            raise RuntimeError(f"Supabase upsert 실패: {err}")

        inserted = 0 if data is None else len(data)
        total_upserted += inserted
        print("UPSERTED:", inserted)

    print("\nTOTAL UPSERTED:", total_upserted)
    print("===== DONE =====")

    # 0건이어도 “정상(그날 거래없음)”일 수 있으니 강제 실패는 하지 않음
    # 만약 실패로 보고 싶으면 아래 주석 해제
    # if total_upserted == 0:
    #     raise RuntimeError("UPSERT가 0건임 (조건/날짜 확인 필요)")


if __name__ == "__main__":
    main()
