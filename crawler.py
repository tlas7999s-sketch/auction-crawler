import os
import requests

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def fetch_data(date_str: str):
    service_key = os.getenv("SERVICE_KEY", "").strip()

    # 1) 서비스키 유무부터 강제 체크
    if not service_key:
        raise RuntimeError("SERVICE_KEY가 비어있음. GitHub Secrets에 SERVICE_KEY가 제대로 등록됐는지 확인!")

    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 50,
        "returnType": "json",

        # 너가 쓰던 조건들 (예시)
        "cond[whsl_mrkt_cd::EQ]": "110001",
        "cond[corp_cd::EQ]": "11000103",
        "cond[gds_lclsf_cd::EQ]": "08",
        "cond[gds_mclsf_cd::EQ]": "03",
        "cond[trd_clcln_ymd::EQ]": date_str,
        # "cond[scsbd_dt::LIKE]": "2026-02",  # 필요하면 유지
    }

    res = requests.get(BASE_URL, params=params, timeout=30)

    # 2) 무조건 상태/콘텐츠 타입/앞부분 출력 (Actions 로그에서 확인)
    print("HTTP:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))
    print("Body(head 300):", res.text[:300])

    # 3) JSON 파싱 시도
    try:
        return res.json()
    except Exception as e:
        raise RuntimeError(f"JSON 파싱 실패: {e}\n응답 앞부분(300): {res.text[:300]}")
