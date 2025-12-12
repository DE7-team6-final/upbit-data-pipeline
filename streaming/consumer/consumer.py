

import json
from confluent_kafka import Consumer, KafkaException
import time
import uuid
from datetime import datetime 
from google.cloud import storage 

BROKER = "localhost:19092"  # Redpanda/Kafka 브로커 주소
TOPIC = "upbit-ticks"       # Producer가 전송하는 토픽 이름
GROUP_ID = "upbit-consumer-group-1"  # Consumer 그룹 ID (동시 소비 제어용)
GCS_BUCKET_NAME="upbit-streaming"


BATCH_SIZE=1000
FLUSH_INTERVAL=60
LATENCY_THRESHOLD_MS = 1000


# Kafka Consumer 설정
CONFIG = {
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",  # 처음 실행 시 가장 처음(offset=0)부터 읽음
}
class GCSUploader:
    def __init__(self, bucket_name):
        self.client=storage.Client()
        self.bucket=self.client.bucket(bucket_name)
        print(f"GCS 버킷 {bucket_name} 연결 성공")

    def upload_json(self, data_list):
        if not data_list:
            return 
        file_content = "\n".join([json.dumps(d, ensure_ascii=False) for d in data_list])

        now=datetime.now()
        folder_path=now.strftime("ticker/dt=%Y-%m-%d")
        file_name = f"ticker_{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}.jsonl"
        blob_path = f"{folder_path}/{file_name}"

        #업로드
        blob=self.bucket.blob(blob_path)
        blob.upload_from_string(file_content, content_type="application/json")
        print(f"GCS 업로드 완료: {blob_path} (건수: {len(data_list)}개)")

def main():
    consumer = Consumer(CONFIG)
    consumer.subscribe([TOPIC])  

    uploader=GCSUploader(GCS_BUCKET_NAME)

    buffer= []
    last_flush_time=time.time()


    print(f"Consumer 시작됨 | Topic: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)  # 1초 대기. 데이터 없으면 none 반환 

            if msg is None:
                #데이터 안들어와도 시간이 지나면 저장해야함 (타임아웃 체크)
                if len(buffer) > 0 and (time.time() - last_flush_time > FLUSH_INTERVAL):
                    uploader.upload_json(buffer)
                    buffer=[]
                    last_flush_time=time.time()
                continue                  

            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue

            try:
                # Value는 JSON 문자열 → dict로 변환
                raw_value = msg.value().decode("utf-8")
                data = json.loads(raw_value)
                market_name = data.get("code", "Unknown")

                if "stream_time" not in data:
                    data["stream_time"] = int(time.time()*1000)
                
                # 지연 계산
                if "stream_time" in data:
                    latency = int(time.time() * 1000) - data["stream_time"]
                    if latency >= LATENCY_THRESHOLD_MS:
                        print(f"MK: {market_name} | Price: {data.get('trade_price')} | Latency: {latency}ms")

                buffer.append(data)

            except Exception as e:
                print(f"데이터 파싱 에러: {e}")
                continue

            current_time=time.time()
            if len(buffer) >= BATCH_SIZE or (current_time - last_flush_time > FLUSH_INTERVAL):
                uploader.upload_json(buffer)
                buffer=[]
                last_flush_time=current_time 

    except KeyboardInterrupt:
        print("\n Consumer 종료 요청됨.")
    finally:
        consumer.close()
        print(" Consumer 커넥션 종료됨.")


if __name__ == "__main__":
    main()