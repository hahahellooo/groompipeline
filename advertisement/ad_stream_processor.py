from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, from_utc_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import window
import redis
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import requests
import json

load_dotenv()

# 스키마 정의
schema = StructType() \
    .add("ad_id", StringType()) \
    .add("event_type", StringType()) \
    .add("user_id", StringType()) \
    .add("timestamp", TimestampType())

# 단가 설정
EVENT_COST = {
    "click": 100,
    "impression": 1
}

# SparkSession 생성
spark = SparkSession.builder \
    .appName("AdBillingProcessor") \
    .getOrCreate()

# Kafka에서 이벤트 읽기
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "3.34.30.146:9092,3.36.10.141:9092,43.203.117.45:9092") \
    .option("subscribe", "ad-events") \
    .load()

df.printSchema()

# Kafka 메시지 파싱
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

deduped_df = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .dropDuplicates(["ad_id", "user_id", "event_type", "timestamp"])  # 중복 제거 기준

# 단가 계산 추가
df_with_cost = parsed_df.withColumn("cost", when(col("event_type") == "click", 100)
                                               .when(col("event_type") == "impression", 1)
                                               .otherwise(0))

# df_with_kst = df_with_cost.withColumn(
#     "timestamp", from_utc_timestamp(col("timestamp"), "Asia/Seoul")
# )

# Kafka 프로듀서 설정 (함수 밖에 생성)
producer = KafkaProducer(
    bootstrap_servers="3.34.30.146:9092,3.36.10.141:9092,43.203.117.45:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_billing_log(ad_id, event_type, cost, timestamp, remaining_budget):
    log = {
        "ad_id": ad_id,
        "event_type": event_type,
        "cost": cost,
        "timestamp": str(timestamp),
        "remaining_budget": remaining_budget
    }
    producer.send("ad-billing-log", value=log)
    producer.flush()

def send_slack_alert(ad_id, current_budget, event_type):
    webhook_url = os.getenv("MY_WEBHOOK_URL")
    message = {
        "text": f":warning: 예산 부족 알림\n*광고 ID:* `{ad_id}`\n*이벤트:* `{event_type}`\n*남은 예산:* `{current_budget}원`"
    }

    try:
        response = requests.post(webhook_url, json=message)
        if response.status_code != 200:
            print(f"❗️Slack 알림 실패: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"❗️Slack 전송 에러: {str(e)}")


# Redis 처리 로직
# Redis Lua 스크립트 (전역 등록용)
LUA_SCRIPT = """
local budget = tonumber(redis.call('get', KEYS[1]) or '0')
local cost = tonumber(ARGV[1])
local alert_ttl = tonumber(ARGV[2])

if budget >= cost then
    redis.call('decrby', KEYS[1], cost)
    if budget - cost >= cost * 2 then
        redis.call('del', KEYS[2])
    end
    return {'OK', budget - cost}
else
    if redis.call('exists', KEYS[2]) == 0 then
        redis.call('setex', KEYS[2], alert_ttl, 1)
        return {'ALERT', budget}
    else
        return {'NOALERT', budget}
    end
end
"""

def process_batch(df, epoch_id):
    print(f"🚀 [epoch {epoch_id}] 배치 처리 시작")

    r = redis.Redis(host="localhost", port=6379, db=0)
    lua_script = r.register_script(LUA_SCRIPT)

    for row in df.collect():
        ad_id = row["ad_id"]
        cost = row["cost"]
        event_type = row["event_type"]
        timestamp = row["timestamp"]
        key = f"ad_budget:{ad_id}"
        alert_key = f"alert_sent:{ad_id}"

        if not r.exists(key):
            print(f"{ad_id} → 예산 없음 (슬랙 전송)")
            send_slack_alert(ad_id, 0, event_type)
            r.setex(alert_key, 86400, 1)
            send_billing_log(ad_id, event_type, 0, timestamp=timestamp, remaining_budget=0)
            continue


        try:
            result = lua_script(keys=[key, alert_key], args=[cost, 86400])  # TTL 1일
            status, remaining = result[0], int(result[1])

            if status == "OK":
                print(f"{ad_id} → {cost}원 차감 → 남은 예산 {remaining}")
                send_billing_log(ad_id, event_type, cost, timestamp=timestamp, remaining_budget=remaining)

            elif status == "ALERT":
                print(f"{ad_id} → 예산 부족 (슬랙 전송)")
                send_slack_alert(ad_id, remaining, event_type)
                send_billing_log(ad_id, event_type, cost, timestamp=timestamp, remaining_budget=remaining)

            elif status == "ALERT_ZERO":
                print(f"{ad_id} → 예산 0원 (슬랙 강제 전송)")
                send_slack_alert(ad_id, 0, event_type)
                send_billing_log(ad_id, event_type, 0, timestamp=timestamp, remaining_budget=0)

            elif status == "NOALERT":
                print(f"{ad_id} → 예산 부족 (슬랙 이미 전송됨)")
                send_billing_log(ad_id, event_type, cost, timestamp=timestamp, remaining_budget=remaining)

        except Exception as e:
            print(f"{ad_id} 처리 중 에러 발생: {str(e)}")


# 스트리밍 시작
query = deduped_df.withColumn("cost", when(col("event_type") == "click", 100)
                                           .when(col("event_type") == "impression", 1)
                                           .otherwise(0)) \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()


query.awaitTermination()
