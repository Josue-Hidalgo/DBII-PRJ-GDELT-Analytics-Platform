"""
gdelt_pipeline_dag.py — DAG de Airflow para el pipeline GDELT
==============================================================
Orquesta el ciclo completo:
  1. Loader: descarga datos GDELT (cada 15 minutos)
  2. Spark: ejecuta todos los análisis una vez que el loader termina
  3. Cleanup: registra la ejecución en MongoDB

Activar el perfil: docker compose --profile airflow up
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ─── Configuración por defecto ────────────────────────────────────────────────

default_args = {
    "owner": "gdelt-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

SPARK_MASTER = "spark://spark-master:7077"
PARQUET_DIR  = "/data/parquet"
MONGO_URI    = "mongodb://{{ var.value.get('mongo_user', 'gdelt_admin') }}:{{ var.value.get('mongo_password', 'changeme') }}@mongodb:27017/gdelt?authSource=admin"


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="gdelt_pipeline",
    description="Descarga GDELT cada 15 min, ejecuta análisis Spark y guarda en MongoDB",
    schedule_interval="*/15 * * * *",   # cada 15 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gdelt", "spark", "mongo"],
) as dag:

    # ─── Tarea 1: Loader ────────────────────────────────────────────────────
    loader_task = BashOperator(
        task_id="run_loader",
        bash_command="python /app/src/loader.py --once",
        doc_md="Descarga los archivos GDELT de los últimos 15 minutos y los convierte a Parquet.",
    )

    # ─── Tarea 2: Análisis de Conflictos ─────────────────────────────────────
    conflicts_task = BashOperator(
        task_id="spark_conflicts",
        bash_command=(
            f"/opt/bitnami/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
            f"--conf 'spark.mongodb.write.connection.uri={MONGO_URI}' "
            f"/opt/spark-jobs/analysis_conflicts.py "
            f"--parquet-dir {PARQUET_DIR} --mongo-uri {MONGO_URI}"
        ),
        doc_md="Ejecuta análisis de conflictos (1, 8, 9, 14, 16) con Spark.",
    )

    # ─── Tarea 3: Análisis de Medios ─────────────────────────────────────────
    media_task = BashOperator(
        task_id="spark_media",
        bash_command=(
            f"/opt/bitnami/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
            f"--conf 'spark.mongodb.write.connection.uri={MONGO_URI}' "
            f"/opt/spark-jobs/analysis_media.py "
            f"--parquet-dir {PARQUET_DIR} --mongo-uri {MONGO_URI}"
        ),
        doc_md="Ejecuta análisis de cobertura mediática (2, 6, 12, 15, 17).",
    )

    # ─── Tarea 4: Análisis de Sentimiento/Actores ────────────────────────────
    sentiment_task = BashOperator(
        task_id="spark_sentiment",
        bash_command=(
            f"/opt/bitnami/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
            f"--conf 'spark.mongodb.write.connection.uri={MONGO_URI}' "
            f"/opt/spark-jobs/analysis_sentiment_actors.py "
            f"--parquet-dir {PARQUET_DIR} --mongo-uri {MONGO_URI}"
        ),
        doc_md="Ejecuta análisis de sentimiento, actores y GKG (3, 4, 5, 7, 10, 11, 13, 18, 19).",
    )

    # ─── Tarea 5: Registrar ejecución ────────────────────────────────────────
    def record_run(**context):
        from pymongo import MongoClient
        import os
        uri = os.getenv("MONGO_URI", "mongodb://gdelt_admin:changeme@mongodb:27017/gdelt?authSource=admin")
        client = MongoClient(uri)
        db = client["gdelt"]
        db["pipeline_runs"].insert_one({
            "dag_run_id": context["run_id"],
            "started_at": context["data_interval_start"],
            "ended_at": datetime.utcnow(),
            "status": "success",
        })
        client.close()

    record_task = PythonOperator(
        task_id="record_pipeline_run",
        python_callable=record_run,
        doc_md="Registra la ejecución exitosa del pipeline en la colección pipeline_runs.",
    )

    # ─── Dependencias ────────────────────────────────────────────────────────
    # loader → [conflicts, media, sentiment] en paralelo → record
    loader_task >> [conflicts_task, media_task, sentiment_task] >> record_task
