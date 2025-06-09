from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
from datetime import datetime, timedelta
# import psycopg2

from utils.slack_fail_noti import task_fail_slack_alert

def ad_events_producer():
    producer = KafkaProducer(bootstrap_servers='3.34.30.146:9092,3.36.10.141:9092,43.203.117.45:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    ad_ids = ["ad_101", "ad_102"]
    event_types = ["impression", "click"]

    def generate_event():
        return {
            "ad_id": random.choice(ad_ids),
            "event_type": random.choices(event_types, weights=[0.8, 0.2])[0],
            "user_id": f"user_{random.randint(1000, 9999)}",
            "timestamp": datetime.utcnow().isoformat()
        }

    count = 0
    while count < 201:
        event = generate_event()
        print("Sending:", event)
        producer.send("ad-events", event)
        count += 1
        time.sleep(0.5)

    producer.flush()
    producer.close()

def consume_billing_log():
    consumer = KafkaConsumer(         
            'ad-billing-log',
            bootstrap_servers='3.34.30.146:9092,3.36.10.141:9092,43.203.117.45:9092',
            group_id='ad-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=5000
        )
  
    try:
        conn = psycopg2.connect(
            dbname='ad_log_db',
            user='postgres',
            password='postgres',
            host='host.docker.internal',
            port=5432
        )
        print("Postgre connection successed")
    except Exception as e:
        print(f"postgres connection fail: {e}")
    
    cursor = conn.cursor()

    
    # Kafka 메시지 처리 및 DB 적재
    for message in consumer:
        log = message.value
        print(len(log))

        try:
            cursor.execute("""
                INSERT INTO ad_billing_logs (ad_id, event_type, cost, timestamp, remaining_budget)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                log['ad_id'],
                log['event_type'],
                log['cost'],
                log['timestamp'],
                log['remaining_budget']
            ))

            conn.commit()
            print(f"✅ 로그 적재 완료: {log['ad_id']} ({log['event_type']})")

        except Exception as e:
            conn.rollback()
            print(f"❌ DB 적재 실패: {e} | 로그: {log}")
    
    conn.close()

def delay_five():
    time.sleep(5*60)
    

# DAG 정의
with DAG(
    dag_id="ad_pipeline",
    start_date=datetime(2025, 5, 25),
    catchup=False,
) as dag:

    ad_producer = PythonOperator(
        task_id='ad_producer',
        python_callable=ad_events_producer,
        on_failure_callback=task_fail_slack_alert  
    )

    delay = PythonOperator(
        task_id='wait_5_minutes',
        python_callable=delay_five
    )

    ad_log_save = PythonOperator(
        task_id='ad_log_save',
        python_callable=consume_billing_log,
        on_failure_callback=task_fail_slack_alert
    )

    ad_producer >> delay >> ad_log_save