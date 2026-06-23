FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    pymongo==4.7.2 \
    pyarrow==14.0.2 \
    pandas==2.0.3

# Descargar MongoDB Spark Connector
RUN wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.1/mongo-spark-connector_2.12-10.2.1.jar -O /opt/spark/jars/mongo-spark-connector.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.10.2/mongodb-driver-sync-4.10.2.jar -O /opt/spark/jars/mongodb-driver-sync.jar

# Crear directorio ivy para dependencias
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R spark:spark /home/spark/.ivy2

COPY jobs/ /opt/spark-jobs/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER 185