FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.9.0 \
    pymongo==4.7.2

COPY dags/ /opt/airflow/dags/

# Inicializar la base de datos de Airflow al arrancar
CMD ["bash", "-c", "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && airflow webserver & airflow scheduler"]
