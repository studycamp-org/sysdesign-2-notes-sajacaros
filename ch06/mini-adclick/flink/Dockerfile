# Flink 2.2 + Kafka SQL 커넥터
FROM flink:2.2.1-java17
USER root
RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
 && chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
 # 체크포인트·세이브포인트를 둘 공유 볼륨 자리 (flink 사용자가 쓸 수 있게)
 && mkdir -p /flink-data/checkpoints /flink-data/savepoints && chown -R flink:flink /flink-data
