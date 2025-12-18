from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import time
import pandas as pd
import boto3
from io import BytesIO

BASE_URL = "https://openapi.koreainvestment.com:9443"

# 수집할 종목 리스트 
TARGET_STOCKS = [
    {"code": "BMNR", "name": "BITMINE IMMERSION TECNOLOGIES", "market":"AMS"}, #아멕스
    {"code": "COIN", "name": "COINBASE GLOBAL", "market":"NAS"}, #나스닥
    {"code": "RIOT", "name": "RIOT PLATFORMS", "market":"NAS"},#나스닥
    {"code": "09399", "name": "CSOP MICROSTRATEGY", "market":"HKS"},#홍콩
    {"code":"QQQ","name":"INVESCO QQQ TRUST", "market":"NAS"} #나스닥
]

def get_access_token(**context):
    """
    한국투자증권 Access Token 발급
    """

    MODE = "PROD" 
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
    
    results = []
    APP_KEY = Variable.get("kis_app_key")
    APP_SECRET = Variable.get("kis_app_secret")
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "HHDFS76200200"  # 주식 현재가 시세 TR ID
    }
    
    for stock in TARGET_STOCKS:
        
        params = {
            "AUTH": "",           
            "EXCD": stock['market'], # NYS(뉴욕), NAS(나스닥), AMS(아멕스)
            "SYMB": stock['code']    # 종목코드 (AAPL, TSLA 등)
        }
        
        url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/price-detail"
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if data['rt_cd'] != '0':
                print(f"API 호출 실패({stock['code']}): {data['msg1']}")
                continue
            output = data['output']
            # 필요한 데이터 정제
            record = {
                'code': output['rsym'],          # 종목코드 (실시간조회종목코드)
                'trade_price': float(output['last']), # 현재가
                'open': float(output['open']),        # 시가
                'high': float(output['high']),        # 고가
                'low': float(output['low']),          # 저가
                'prev_close': float(output['base']),  # 전일종가
                'volume': float(output['tvol']),      # 거래량 (문서 하단 tvol 사용)
                'trade_amount': float(output['tamt']),# 거래대금
                'market_cap': float(output['tomv']),  # 시가총액
                'per': float(output['perx']),         # PER
                'pbr': float(output['pbrx']),         # PBR
                'trade_date': datetime.now().strftime("%Y%m%d"), # 수집일자
                'collected_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            results.append(record)
            print(f"수집 성공: {stock['name']}")
        else:
            print(f"수집 실패: {stock['name']} - {res.text}")
            
        time.sleep(0.2)
        
    return results

def upload_to_s3(**context):
    """
    수집된 데이터를 Parquet으로 변환해 S3 업로드
    """
    data = context['task_instance'].xcom_pull(task_ids='fetch_price_task')
    
    if not data:
        print("수집된 데이터가 없습니다.")
        return

    df = pd.DataFrame(data)
    
    
    now = datetime.now()
    file_name = f"{now.strftime('%Y-%m-%d')}.parquet"
    s3_key = f"kis_stock/{file_name}"
    bucket_name = "team6-batch"  

    aws_access_key = Variable.get("aws_access_key_id")
    aws_secret_key = Variable.get("aws_secret_access_key")

    # 메모리에서 Parquet 변환 후 업로드
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=out_buffer.getvalue())
    
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
    description='한투 API 주식 현재가 수집',
    schedule='10 9 * * 1-5', # 평일 9시 10분마다 실행
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