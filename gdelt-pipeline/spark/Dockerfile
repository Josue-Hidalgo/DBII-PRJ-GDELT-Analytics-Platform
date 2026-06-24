FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    pymongo==4.7.2 \
    pyarrow==14.0.2 \
    pandas==2.0.3

# Crear directorio ivy para dependencias
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R spark:spark /home/spark/.ivy2

COPY jobs/ /opt/spark-jobs/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER 185