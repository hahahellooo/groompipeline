from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from kafka import KafkaProducer, KafkaConsumer
import json
import csv
import random
import time
from io import StringIO, BytesIO
from faker import Faker
from datetime import datetime, timezone, timedelta


fake = Faker()

def generate_log():
    # 사용자 행동 로그 샘플 정의
    event_types = ['movie_click', 'like_click', 'ad_click', 'play_start', 'play_complete']
    pages = ['main', 'movie_detail', 'campaign']
    categories = ['action', 'comedy', 'drama', 'sci-fi']
    
    now = datetime.utcnow()
    return {
        "user_id": fake.uuid4(),
        "movie_id": f"m{random.randint(100,999)}",
        "timestamp": now.isoformat(),
        "event_type": random.choice(event_types),
        "page": random.choice(pages),
        "movie_category": random.choice(categories),
        "utm_source": random.choice(['instagram', 'naver', 'youtube']),
        "utm_medium": random.choice(['social', 'banner', 'cpc']),
        "utm_campaign": random.choice(['spring_sale', 'launch2025']),
        "utm_content": random.choice(['blue_button', 'video_ad'])
    }

def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='host.docker.internal:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for i in range(100):
        event = generate_log()
        producer.send('airflowtest', event)
        print(f"✅ Sent: {event['event_type']} from {event['utm_source']}")
        time.sleep(0.05)  # 속도 조절

    producer.flush()


def kafka_consumer():
    consumer = KafkaConsumer(
        'airflowtest',
        bootstrap_servers='host.docker.internal:9092',
        group_id='spark',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )

    try:
        s3_hook = S3Hook(
        aws_conn_id='minio',
        region_name='us-east-1'
        )
    except Exception as e:
        print(f"MinIO 연결 실패: {e}")
        return

    print("✅ Consumer started, waiting for messages...")

    # 한국 시간(KST)으로 현재 시간 가져오기
    kst = timezone(timedelta(hours=9))  # KST는 UTC +9 시간대
    current_time_kst = datetime.now(kst)

    # msg_count = 0
    # max_msg = 50

    message = consumer.poll(timeout_ms=5000)
    
    # 메세지 없으면 종료
    if not message:
        print("No message!!!!")

    csv_file = StringIO()
    fieldnames = ["user_id", "movie_id", "timestamp", "event_type", "page", "movie_category", "utm_source", "utm_medium", "utm_campaign", "utm_content"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()

    msg_count = 0
    max_msg = 100

    for tp, messages in message.items():
        for msg in messages:
            event = msg.value
            writer.writerow({key: event[key] for key in fieldnames})
            print(f"📥 Received: {event['event_type']} from {event['utm_source']} at {event['timestamp']}")

            msg_count += 1

        if msg_count >= max_msg:
            consumer.commit()
            print(f"✅ Committed {msg_count} messages.")
            msg_count = 0

    # csv를 minio에 업로드(메모리상에 기록된 csv를)
    csv_file.seek(0) # 파일 포인터를 처음으로 되돌림

    # StringIO 객체를 바이트형으로 변환하고, 이를 BytesIO 객체로 감쌈
    csv_data = csv_file.getvalue().encode('utf-8')  # 문자열을 바이트로 변환        
    csv_stream = BytesIO(csv_data)  # 바이트 데이터를 BytesIO 객체로 변환

    bucket_name = "user-activity-log"
    # 한국 시간으로 파일명 생성 (예: 2025-05-13_13-30-00.csv)
    filename = current_time_kst.strftime("%Y-%m-%d_%H-%M-%S") + ".csv"

    s3_hook.load_file_obj(csv_stream, filename, bucket_name, replace=True)
    print(f"✅ File uploaded to MinIO: {filename}")
    
    consumer.close()
    

with DAG(
    'csv_to_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5),
        'execution_timeout': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval='5 * * * *',
    start_date=datetime(2025, 5, 13),
    catchup=False,
    tags=['user_activity_log', 'ott', 'spark', 'minio'],
) as dag:
    
    kafka_producer = PythonOperator(
        task_id='kafka_producer',
        python_callable=kafka_producer
    )

    kafka_consumer = PythonOperator(
        task_id='kafka_consumer',
        python_callable=kafka_consumer
    )

    csv_to_parquet = SSHOperator(
    task_id='csv_to_parquet',
    ssh_conn_id="local_ssh",
    command='sh -c "/Users/jeongmieun/.pyenv/versions/airminio/bin/python /Users/jeongmieun/test/docker_***/dags/spark_to_parquet.py"'
    )


    kafka_producer >> kafka_consumer >> csv_to_parquet


    
    

