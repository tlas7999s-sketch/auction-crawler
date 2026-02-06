import os
import requests

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def fetch_data(date_ymd: str):
    service_key = os.getenv("SERVICE_KEY")
    if not service_key:
        raise RuntimeError("SERVICE_KEY 환경변수가 비어있습니다. GitHub Actions Secret 이름 확인 필요.")

    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 50,
        "returnType": "json",

        # 예시: 네가 쓰던 조건들
        "cond[trd_clcln_ymd::EQ]": date_ymd,
        # "cond[whsl_mrkt_cd::EQ]": "110001",
        # "cond[corp_cd::EQ]": "11000103",
        # "cond[gds_lclsf_cd::EQ]": "08",
        # "cond[gds_mclsf_cd::EQ]": "03",
        # "cond[scsbd_dt::LIKE]": "2026-02",
    }

    res = requests.get(BASE_URL, params=params, timeout=30)

    print("HTTP:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))
    print("URL:", res.url)
    print("BODY(head 300):", res.text[:300])

    res.raise_for_status()

    # JSON 아닐 때를 대비
    try:
        return res.json()
    except Exception as e:
        raise RuntimeError(f"JSON 파싱 실패. 응답이 JSON이 아닙니다. body 일부: {res.text[:300]}") from e
