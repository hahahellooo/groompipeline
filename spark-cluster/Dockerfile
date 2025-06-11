FROM spark:3.5.5-scala2.12-java17-ubuntu

USER root

# Python3 및 의존성 설치
RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip curl wget; \
    rm -rf /var/lib/apt/lists/*

# 필요한 JAR 다운로드
ENV SPARK_HOME=/opt/spark

RUN wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

USER spark
