from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import time
import pandas as pd
import pytz

from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BASE_URL = "https://openapi.koreainvestment.com:9443"

# 수집할 종목 리스트 
TARGET_STOCKS = [
    {"code": "BMNR", "name": "BITMINE IMMERSION TECNOLOGIES", "market":"AMS"}, #아멕스
    {"code": "COIN", "name": "COINBASE GLOBAL", "market":"NAS"}, #나스닥
    {"code": "RIOT", "name": "RIOT PLATFORMS", "market":"NAS"},#나스닥
    {"code": "BITO", "name": "PROSHARES BITCOIN", "market":"AMS"},#아멕스
    {"code":"QQQ","name":"INVESCO QQQ TRUST", "market":"NAS"} #나스닥
]

def get_access_token(**context):
    """
    한국투자증권 Access Token 발급
    """

    APP_KEY = Variable.get("kis_app_key")
    APP_SECRET = Variable.get("kis_app_secret")
    url = f"{BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    
    res = requests.post(url, headers=headers, data=json.dumps(body))
    res_json = res.json()
    
    if res.status_code != 200:
        raise Exception(f"Token 발급 실패: {res_json}")
    
    token = res_json['access_token']
    print("Token 발급 성공")
    
    # 다음 Task로 토큰 넘기기 (XCom)
    return token

def fetch_stock_prices(**context):
    """
    종목별 현재가 조회 (Rate Limit 고려하여 순차 호출)
    """
    # 이전 Task에서 토큰 받아오기
    access_token = context['task_instance'].xcom_pull(task_ids='get_token_task')
    
    APP_KEY = Variable.get("kis_app_key")
    APP_SECRET = Variable.get("kis_app_secret")

    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    target_date = (now_kst - timedelta(days=1)).strftime("%Y%m%d")
    print(f"수집 대상 날짜 (KST 전일): {target_date}")

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "HHDFS76950200"  # 주식 현재가 시세 TR ID
    }

    all_results = []

    for stock in TARGET_STOCKS:
        stock_data = []
        params = {
            "AUTH": "",
            "EXCD": stock['market'], # 거래소 코드 (NAS, AMS)
            "SYMB": stock['code'],   # 종목 코드
            "NMIN": "1",             # 1분봉
            "PINC": "1",
            "NEXT": "",              # 다음 페이지 여부
            "NREC": "120",           # 요청당 개수 (최대 120)
            "FILL": "",
            "KEYB": ""
        }
        
        url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
        
        while True:
            try:
                res = requests.get(url, headers=headers, params=params)
                
                if res.status_code != 200:
                    print(f"API 호출 실패({stock['code']}): {res.text}")
                    break
                data = res.json()
                if data['rt_cd'] != '0':
                    print(f"API 호출 실패({stock['code']}): {data['msg1']}")
                    break
                bars = data.get("output2", [])
                if not bars:
                    break

                stop_collection=False
                for bar in bars: 
                    current_date=bar.get("tymd")

                    if current_date == target_date: 
                        record = {
                        'code': stock['code'],
                        'name': stock['name'],
                        'market': stock['market'],
                        'trade_date': bar['tymd'],      # 영업일자
                        'trade_time': bar['xhms'],      # 체결시간(HHMMSS)
                        'open': float(bar['open']),     # 시가
                        'high': float(bar['high']),     # 고가
                        'low': float(bar['low']),       # 저가
                        'close': float(bar['last']),    # 종가 (API 응답 키 'last')
                        'volume': float(bar['evol']),   # 거래량
                        'collected_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        stock_data.append(record)
                    elif current_date < target_date:
                        stop_collection=True
                        break 
                if stop_collection:
                    print(f"  -> {target_date} 이전 데이터 도달. 수집 종료.")
                    break 
                if len(bars) < 2: # 데이터가 거의 없으면 중단
                    break
                last_bar = bars[-1]
                
                params["NEXT"] = "1" # 다음 페이지 조회 시 필수
                # KEYB는 YYYYMMDD + HHMMSS (반드시 이전 응답의 마지막 데이터 기준)
                params["KEYB"] = (last_bar.get("xymd") or last_bar.get("tymd")) + last_bar.get("xhms")


                time.sleep(0.2)
            except Exception as e:
                print(f"Loop 중 에러 발생: {e}")
                break 
        print(f"수집 성공: {stock['name']}")
        all_results.extend(stock_data)
        time.sleep(0.5) # 종목 간 딜레이
        
    return all_results

def upload_to_s3(**context):
    """
    수집된 데이터를 Parquet으로 변환해 S3 업로드
    """
    data = context['task_instance'].xcom_pull(task_ids='fetch_price_task')
    
    if not data:
        print("수집된 데이터가 없습니다.")
        return

    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=['code', 'trade_time'], keep='first')
    
    # 3. 정규장 시간 필터링 함수 정의 (09:30 ~ 16:00)
    def is_market_open(row):
        t = row['trade_time']        
        return "093000" <= t <= "160000"

    # 필터링 적용
    df = df[df.apply(is_market_open, axis=1)]
    print(df.groupby('code')['trade_time'].count())
    
    now = datetime.now()
    file_name = "stock.parquet"

    s3_key = f"yymmdd={now.strftime('%Y-%m-%d')}/{file_name}"
    bucket_name = "team6-batch"  

    # 메모리에서 Parquet 변환 후 업로드
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')

    s3_hook.load_bytes(
        bytes_data=out_buffer.getvalue(),
        key=s3_key,
        bucket_name=bucket_name,
        replace=True  # 덮어쓰기 허용 (재실행 시 에러 방지)
    )
    
    print(f"S3 업로드 완료: s3://{bucket_name}/{s3_key}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'kis_stock_batch_loader',
    default_args=default_args,
    description='한투 API 주식 1분봉 수집',
    schedule='1 0 * * 1-5', # kst 9시 1분 시작 
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['kis', 'stock', 'batch']
) as dag:

    t1 = PythonOperator(
        task_id='get_token_task',
        python_callable=get_access_token
    )

    t2 = PythonOperator(
        task_id='fetch_price_task',
        python_callable=fetch_stock_prices
    )

    t3 = PythonOperator(
        task_id='save_to_s3_task',
        python_callable=upload_to_s3
    )

    # 순서 정의
    t1 >> t2 >> t3