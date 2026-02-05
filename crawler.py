import requests
import datetime
import os

SERVICE_KEY = os.getenv("SERVICE_KEY")

URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def fetch_data(date):
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 100,
        "returnType": "json",
        "cond[whsl_mrkt_cd::EQ]": "",
        "cond[gds_lclsf_cd::EQ]": "08",
        "cond[gds_mclsf_cd::EQ]": "03",
        "cond[trd_clcln_ymd::EQ]": date,
        "cond[scsbd_dt::LIKE]": date[:7]
    }

    res = requests.get(URL, params=params)
    return res.json()

def main():
    today = datetime.date.today().strftime("%Y-%m-%d")
    data = fetch_data(today)
    print(data)

if __name__ == "__main__":
    main()
