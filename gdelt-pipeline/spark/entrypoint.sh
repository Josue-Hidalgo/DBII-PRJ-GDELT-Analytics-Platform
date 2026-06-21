#!/bin/bash
set -e

# Atajo: docker compose exec spark-master /entrypoint.sh run-analysis
# Corre los 3 jobs de análisis GDELT en secuencia contra el cluster ya activo.
if [ "$1" = "run-analysis" ]; then
    SPARK_SUBMIT="/opt/spark/bin/spark-submit"
    MASTER_URL="spark://spark-master:7077"
    MONGO_PKG="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    JOBS_DIR="/opt/spark-jobs"

    for job in analysis_conflicts.py analysis_media.py analysis_sentiment_actors.py; do
        echo "═══ Ejecutando ${job} ═══"
        "$SPARK_SUBMIT" --master "$MASTER_URL" --packages "$MONGO_PKG" "${JOBS_DIR}/${job}"
    done

    echo "═══ Todos los análisis finalizados (run-analysis) ═══"
    exit 0
fi

# Si se pasa un comando explícito (como hace docker-compose), ejecutarlo directamente
exec "$@"