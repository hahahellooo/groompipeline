from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import json
import random
import string
import os
import time
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from faker import Faker
from kafka.structs import OffsetAndMetadata
from kafka import KafkaConsumer, KafkaProducer
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

# MongoDB 연결을 위한 pymongo import
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    MongoClient = None
    ConnectionFailure = None

from utils.slack_fail_noti import task_fail_slack_alert

kafka_cluster = '43.201.43.88:9092,15.165.234.219:9092,3.35.228.177:9092'

def generate_event(**kwargs):
    # Imports moved to top level

    fake = Faker()
    producer = KafkaProducer(
        bootstrap_servers=kafka_cluster,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=10,
        linger_ms=200,
        batch_size=262144,
        max_request_size=10485760,
        request_timeout_ms=120000,
        buffer_memory=134217728,
        metadata_max_age_ms=30000
    )

    mongo_contents_data = []
    if PYMONGO_AVAILABLE:
        try:
            # MongoDB 연결 정보 - 실제 환경에 맞게 수정하세요.
            # 예: client = MongoClient('mongodb://user:pass@host:port/admin')
            client = MongoClient('mongodb+srv://user:goorm0508@goorm-mongodb.svz66jf.mongodb.net/?retryWrites=true&w=majority&appName=goorm-mongoDB') # 로컬 MongoDB 예시
            client.admin.command('ping') # 연결 테스트
            db = client['content-db']
            contents_collection = db['contents']
            # 'title'과 'videoId' 필드만 가져옵니다. _id는 제외합니다.
            # 실제 MongoDB의 필드명이 'videoId'가 아니라면 해당 필드명으로 수정해야 합니다.
            mongo_contents_data = list(contents_collection.find({}, {"_id": 0, "title": 1, "videoId": 1}))
            client.close()
            if mongo_contents_data:
                print(f"✅ Successfully fetched {len(mongo_contents_data)} items from MongoDB 'contents' collection.")
            else:
                print("ℹ️ No data fetched from MongoDB 'contents' collection or collection is empty.")
        except ConnectionFailure:
            print("❌ Failed to connect to MongoDB. Will proceed without MongoDB data.")
        except Exception as e:
            print(f"❌ Error fetching data from MongoDB: {e}. Will proceed without MongoDB data.")

    topic = 'test-5'
    num_events = 22_000_000  # 필요한 양으로 조절 가능

    event_types = ["like_click", "content_click", "review_write", "rating_submit"]
    pages = ["content_detail", "main"]

    def make_event():
        event = {}
        
        # MongoDB에서 가져온 데이터가 있으면 사용, 없으면 랜덤 생성
        if mongo_contents_data:
            selected_content = random.choice(mongo_contents_data)
            event["videoId"] = selected_content.get("videoId")
            event["title"] = selected_content.get("title")
        else:
            event["videoId"] = None
            # event["title"] = "N/A" # MongoDB 데이터가 없을 경우 기본 타이틀 또는 생략

        event.update({
            "userId": fake.uuid4(),
            "timestamp": fake.date_time_between(start_date="-1d", end_date="now").isoformat() + "Z",
            "eventType": random.choice(event_types),
            "page": random.choice(pages),
        })

        if event["eventType"] == "like_click":
            event["liked"] = random.choice([True, False])
        elif event["eventType"] == "review_write":
            event["review"] = fake.sentence()
        elif event["eventType"] == "rating_submit":
            event["rating"] = random.randint(1, 5)
        else:
            event["contentCategory"] = [fake.word() for _ in range(random.randint(1, 3))]
        return event        # MongoDB에서 가져온 데이터가 있으면 사용, 없으면 랜덤 생성

    print(f"🚀 Producing {num_events:,} dummy messages to Kafka topic `{topic}`")

    start = time.time()
    total_bytes = 0
    for _ in range(num_events):
        msg = make_event()
        json_data = json.dumps(msg).encode("utf-8")
        total_bytes += len(json_data)
        producer.send(topic, value=msg)

    producer.flush()
    print(f"✅ Sent {num_events:,} events in {time.time() - start:.2f} seconds")

    # 크기 출력
    mb = total_bytes / (1024 ** 2)
    gb = total_bytes / (1024 ** 3)
    print(f"✅ Total bytes sent: {total_bytes:,} bytes")
    print(f"✅ ≈ {mb:.2f} MB")
    print(f"✅ ≈ {gb:.2f} GB")

    print("✅ Dummy events sent to Kafka")


def connect_minio():
    
    s3_hook = S3Hook(
        aws_conn_id='minio',
        region_name='us-east-1'
    )

    return s3_hook

def kafka_consumer(**context):
    consumer = None  # Initialize consumer for the finally block
    try:
        consumer = KafkaConsumer(
            'test-5',
            bootstrap_servers=kafka_cluster,
            group_id='test',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
        )

        print("✅ consumer started, waiting for messages....")

        # 메시지 수집 로직 개선
        last_message_received_time = time.time()
        
        # 수집 관련 설정값 (필요에 따라 조정)
        # 짧은 주기로 반복 폴링하여 메시지를 적극적으로 수집
        POLL_TIMEOUT_MS = 1000  # 개별 poll 호출의 타임아웃 (ms)
        # 마지막 메시지를 받고 이 시간동안 추가 메시지가 없으면 수집 종료
        IDLE_CONSUMPTION_TIMEOUT_S = 15 
        # 최대 메시지 수집 시간 (무한정 실행 방지)
        MAX_COLLECTION_DURATION_S = 3600

        collection_start_time = time.time()
        print(f"🚀 Starting message collection for up to {MAX_COLLECTION_DURATION_S}s or until idle for {IDLE_CONSUMPTION_TIMEOUT_S}s.")

        s3_hook = connect_minio()
        print("✅ MinIO connected")

        bucket_name = "userlog-data"
        if not s3_hook.check_for_bucket(bucket_name):
            print(f"No Bucket: ✅{bucket_name} is creating...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created.")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists.")

        current_time_kst = datetime.now(ZoneInfo("Asia/Seoul"))
        base_filename = current_time_kst.strftime("%Y-%m-%d_%H-%M-%S")
        local_json_filename = f"{base_filename}.json"
        s3_raw_object_key = f"test/user-activity-raw/{local_json_filename}"

        offsets_to_commit = {}
        messages_processed_count = 0

        with open(local_json_filename , "w") as f:
            while True:
                if time.time() - collection_start_time > MAX_COLLECTION_DURATION_S:
                    print(f"ℹ️ Max collection duration of {MAX_COLLECTION_DURATION_S}s reached.")
                    break

                messages_batch = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

                if not messages_batch:
                    if time.time() - last_message_received_time > IDLE_CONSUMPTION_TIMEOUT_S:
                        print(f"ℹ️ No messages received for {IDLE_CONSUMPTION_TIMEOUT_S}s. Finalizing batch.")
                        break 
                    continue
                
                last_message_received_time = time.time()
                for tp, msgs_in_partition in messages_batch.items():
                    for msg_data in msgs_in_partition:
                        try:
                            event = msg_data.value
                            json.dump(event, f)
                            f.write("\n")
                            messages_processed_count += 1
                            
                            current_offset_for_tp = offsets_to_commit.get(tp)
                            if current_offset_for_tp is None or msg_data.offset + 1 > current_offset_for_tp.offset:
                                offsets_to_commit[tp] = OffsetAndMetadata(msg_data.offset + 1, None)
                        except Exception as e:
                            print(f"❌ Failed to process message for writing: {e}")
                            raise e

        if messages_processed_count == 0:
            print("❌ No messages processed during the collection period.")
            if os.path.exists(local_json_filename):
                try:
                    os.remove(local_json_filename)
                    print(f"🗑️ Removed empty local file: {local_json_filename}")
                except OSError as e_os:
                    print(f"⚠️ Error removing empty local file {local_json_filename}: {e_os}")
            raise Exception("No messages processed. Topic might be empty or consumer issue.")

        print(f"✅ Successfully wrote {messages_processed_count} messages to {local_json_filename}")

        if messages_processed_count > 0:
            s3_hook.load_file(local_json_filename, s3_raw_object_key, bucket_name, replace=True)
            print(f"File {local_json_filename} uploaded to MinIO: s3://{bucket_name}/{s3_raw_object_key}")
        
        if offsets_to_commit:
            consumer.commit(offsets=offsets_to_commit)
            print(f"✅ Committed offsets: {offsets_to_commit}")

        # Push the S3 object key and base filename (for Spark output) to XCom
        context['ti'].xcom_push(key='s3_raw_object_key', value=s3_raw_object_key)
        context['ti'].xcom_push(key='base_filename_for_processed', value=base_filename)

    except Exception as e:
        print(f"❌ DAG failed due to : {e}")
        raise e
    
    finally:
        if consumer:
            consumer.close()


def check_kafka_broker_health():
    brokers = [kafka_cluster]
    brokers = kafka_cluster.split(',')
    alive_count = 0

    for broker in brokers:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, 
                                     request_timeout_ms=5000)
            admin.list_topics()
            alive_count += 1
            print(f"✅ Broker {broker} is alive")
        except KafkaError as e:
            print(f"❌ Broker {broker} failed: {e}")
        finally:
            try:
                admin.close()
            except NameError: # If admin was not initialized due to an early error
                pass
            except KafkaError as ke: # Or specific Kafka errors if close() can raise them
                pass

    if alive_count < 2:
        raise Exception(f"Kafka 브로커가 {alive_count}개만 살아있습니다. 최소 2개 이상 필요합니다.")



with DAG(
    'test_one_node',
    default_args={
        'depends_on_past':False,
        'retries':2,
        'retry_delay':timedelta(minutes=5),
        'execution_timeout':timedelta(minutes=360),
    },
    description="groomplay",
    start_date=datetime(2025, 5, 19),
    catchup=False,
    schedule_interval='0 0 * * *',
    tags=['spark', 'local']
) as dag:
    
    execution_date = "{{ ds }}"

    dummy_data = PythonOperator(
        task_id='produce_dummy_data',
        python_callable=generate_event,
        on_failure_callback=task_fail_slack_alert
    )
    
    check_kafka_brokers = PythonOperator(
        task_id='check_kafka_broker_health',
        python_callable=check_kafka_broker_health,
        on_failure_callback=task_fail_slack_alert   
    )

    kafka_consumer = PythonOperator(
        task_id='kafka_consumer',
        python_callable=kafka_consumer,
        on_failure_callback=task_fail_slack_alert
    )

    # 업로드 여부 확인 
    check_minio_file = S3KeySensor(
        task_id='check_minio_file',
        bucket_name='userlog-data', # kafka_consumer에서 사용하는 버킷 이름과 일치
        bucket_key="{{ ti.xcom_pull(task_ids='kafka_consumer', key='s3_raw_object_key') }}",
        wildcard_match=True,
        aws_conn_id='minio',
        poke_interval=5,
        on_failure_callback=task_fail_slack_alert
    )
  
    # 단일 노드 
    spark_etl = SparkSubmitOperator(
        task_id='spark_etl',
        application="/opt/spark/data/userlog_spark.py",
        conn_id='spark',
        application_args=[
            "--input_path", f"s3a://userlog-data/{{{{ ti.xcom_pull(task_ids='kafka_consumer', key='s3_raw_object_key') }}}}",
            "--output_path", f"s3a://userlog-data/test/ml-learning-data/{{{{ ti.xcom_pull(task_ids='kafka_consumer', key='base_filename_for_processed') }}}}.parquet"
        ],
        jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
        on_failure_callback=task_fail_slack_alert
    )


    dummy_data >> check_kafka_brokers >> kafka_consumer >> check_minio_file >> spark_etl 
