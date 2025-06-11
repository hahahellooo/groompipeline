from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import json
import time
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kafka.structs import OffsetAndMetadata
from kafka import KafkaConsumer, KafkaProducer
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

from utils.slack_fail_noti import task_fail_slack_alert


def generate_event(**kwargs):
    from kafka import KafkaProducer
    from faker import Faker
    import json
    import random
    import string
    import time

    fake = Faker()
    producer = KafkaProducer(
        bootstrap_servers='3.38.204.173:9092,16.184.30.150:9092,16.184.31.12:9092',
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

    topic = 'dummy'
    num_events = 40_000_000  # 필요한 양으로 조절 가능

    event_types = ["like_click", "content_click", "content_recom_click"]
    pages = ["content_detail", "main"]

    def random_video_id():
        return ''.join(random.choices(string.ascii_letters + string.digits, k=11))

    def make_event():
        event = {
            "userId": fake.uuid4(),
            "videoId": random_video_id(),
            "timestamp": fake.date_time_between(start_date="-1d", end_date="now").isoformat() + "Z",
            "eventType": random.choice(event_types),
            "page": random.choice(pages)
        }
        if event["eventType"] == "like_click":
            event["liked"] = random.choice([True, False])
        else:
            event["contentCategory"] = [fake.word() for _ in range(random.randint(1, 3))]
        return event

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
    try:
        consumer = KafkaConsumer(
            'dummy',
            bootstrap_servers='3.38.204.173:9092,16.184.30.150:9092,16.184.31.12:9092',
            group_id='test',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
        )

        print("✅ consumer started, waiting for messages....")

        MAX_RETRIES = 3
        RETRY_DELAY_SEC = 10

        for attempt in range(0, MAX_RETRIES+1):
            messages = consumer.poll(timeout_ms=10000)
            if messages:
                print(f"✅ Messages received on attempt {attempt+1}")
                break
            else:
                print(f"🔁 Attempt {attempt+1}: No messages, retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
        else:
            raise Exception("❌ No messages received after multiple retries. Broker may be down.")

        try:
            s3_hook = connect_minio()
            print("✅Minio connected")
        except Exception as e:
            print("❌ Failed to connect to MinIO")

        bucket_name = "test-biglog-data"

        if not s3_hook.check_for_bucket(bucket_name):
            print(f"No Bucket: ✅{bucket_name} is creating...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created.")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists.")

        # DAG 실행시간으로 파일명 지정
        execution_date = context['execution_date'].astimezone(ZoneInfo("Asia/Seoul"))
        filename = execution_date.strftime("%Y-%m-%d_%H-%M-%S") + ".json"

        with open(filename, "w") as f:
            for tp, message in messages.items():
                for msg in message:
                    try:
                        event = msg.value
                        json.dump(event, f)
                        f.write("\n")
                        print(f"📥 Received: page:{event.get('page')}")
                        consumer.commit(offsets={tp: OffsetAndMetadata(msg.offset + 1, None)})
                    except Exception as e:
                        print(f"❌ Failed to process message: {e}")
                        raise e

        s3_hook.load_file(filename, filename, bucket_name, replace=True)
        print(f"File uploaded to MinIO: {filename}")

    except Exception as e:
        print(f"❌ DAG failed due to : {e}")
        raise e
    
    finally:
        consumer.close()


def check_kafka_broker_health():
    brokers = ["3.38.204.173:9092","16.184.30.150:9092","16.184.31.12:9092"]
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
            except:
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
    schedule_interval='0 1 * * *',
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
        bucket_name='ml-user-log',
        bucket_key='*.json',
        wildcard_match=True,
        aws_conn_id='minio',
        poke_interval=5,
        on_failure_callback=task_fail_slack_alert
    )
  
    # 단일 노드 
    spark_etl = SparkSubmitOperator(
        task_id='spark_etl',
        application="/opt/spark/data/bigdata_userlog_spark.py",
        conn_id='spark',
        jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/postgresql-42.7.4.jar",
        on_failure_callback=task_fail_slack_alert
    )


    dummy_data >> check_kafka_brokers >> kafka_consumer >> check_minio_file >> spark_etl 
