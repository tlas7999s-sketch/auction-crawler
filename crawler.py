import os
import requests
from datetime import datetime, timezone, timedelta

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def fetch_data(date_str: str):
    service_key = os.getenv("SERVICE_KEY", "").strip()

    # 1) 서비스키 유무 체크
    if not service_key:
        raise RuntimeError("SERVICE_KEY가 비어있음. GitHub Secrets에 SERVICE_KEY가 제대로 등록됐는지 확인!")

    params = {
        "serviceKey": service_key,
        "pageNo": 1,
        "numOfRows": 50,
        "returnType": "json",

        # 예시 조건(현재는 부산/대저로 보이는 코드값들)
        "cond[whsl_mrkt_cd::EQ]": "110001",
        "cond[corp_cd::EQ]": "11000103",
        "cond[gds_lclsf_cd::EQ]": "08",
        "cond[gds_mclsf_cd::EQ]": "03",
        "cond[trd_clcln_ymd::EQ]": date_str,
    }

    res = requests.get(BASE_URL, params=params, timeout=30)

    # ★ 디버깅 핵심: 실제 호출 URL 확인
    print("REQUEST URL:", res.url)

    # ★ 디버깅: 상태/타입/응답 앞부분
    print("HTTP:", res.status_code)
    print("Content-Type:", res.headers.get("Content-Type"))
    print("Body(head 300):", res.text[:300])

    # 2) JSON 파싱 시도 (실패하면 보통 XML/HTML 에러)
    try:
        data = res.json()
    except Exception as e:
        raise RuntimeError(
            f"JSON 파싱 실패: {e}\n"
            f"응답 앞부분(300): {res.text[:300]}"
        )

    # 3) 응답 구조 요약 출력 (API마다 구조가 다를 수 있어서)
    print("TOP KEYS:", list(data.keys())[:20])

    # 보통 공공데이터포털은 response/header/body 형태가 많음
    resp = data.get("response")
    if isinstance(resp, dict):
        header = resp.get("header", {})
        body = resp.get("body", {})
        print("HEADER:", header)

        # item이 어디에 들어오는지 확인
        items = None
        if isinstance(body, dict):
            items = body.get("items")
            # items가 dict면 또 item 키가 있을 수 있음
            if isinstance(items, dict) and "item" in items:
                items = items["item"]

        if items is None:
            print("ITEMS: (없음) / body keys:", list(body.keys())[:20] if isinstance(body, dict) else type(body))
        else:
            # items가 list인지 dict인지 출력
            if isinstance(items, list):
                print("ITEMS COUNT:", len(items))
                if len(items) > 0:
                    print("FIRST ITEM:", items[0])
            else:
                print("ITEMS TYPE:", type(items))
                print("ITEMS SAMPLE:", items)

    return data


def main():
    # 한국 날짜 기준 "오늘" (UTC+9)
    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).strftime("%Y-%m-%d")

    print("===== START =====")
    print("DATE:", today)

    data = fetch_data(today)

    print("===== DONE =====")
    # 필요하면 여기서 파일로 저장하거나 DB 저장 단계로 확장


if __name__ == "__main__":
    main()
