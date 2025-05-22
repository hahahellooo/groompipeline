FROM apache/airflow:2.10.1-python3.10

USER root

# Java 17 및 기타 시스템 도구 설치
RUN apt update && apt install -y openjdk-17-jdk procps

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
