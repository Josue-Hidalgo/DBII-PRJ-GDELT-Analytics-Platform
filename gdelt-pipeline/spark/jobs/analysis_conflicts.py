"""
analysis_conflicts
================================================
Análisis incluidos:
  1. Mapa de calor de intensidad de conflictos por país/día (escala Goldstein)
  8. Pares de países que entran en conflicto con mayor frecuencia
  9. Detección de escalada de eventos (aumento acelerado de menciones en 24h)
 14. Grafo de red: interacciones diplomáticas vs. conflictos entre países
 16. Frecuencia de conflictos por etnia de los actores

Resultado: escrito en colecciones MongoDB separadas por análisis.

MongoDB schemas:
  conflict_heatmap    → {country, date, avg_goldstein, event_count}
  conflict_pairs      → {actor1_country, actor2_country, conflict_count, date}
  escalation_events   → {event_id, country, date, mention_spike, hours_to_100}
  diplomatic_network  → {source_country, target_country, diplomatic_count, conflict_count}
  ethnic_conflicts    → {ethnicity, event_count, avg_goldstein, date}
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import create_session, get_args, write_to_mongo, read_events


def run_heatmap(events_df, spark):
    """
    Análisis 1: Mapa de calor — promedio de GoldsteinScale por país/día.
    GoldsteinScale va de -10 (máximo conflicto) a +10 (máxima cooperación).
    Filtramos QuadClass >= 3 para quedarnos solo con eventos de conflicto material.
    """
    result = (
        events_df
        .filter(F.col("QuadClass") >= 3)
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("ActionGeo_CountryCode", "date_clean")
        .agg(
            F.avg("GoldsteinScale").alias("avg_goldstein"),
            F.count("GlobalEventID").alias("event_count"),
            F.avg("NumMentions").alias("avg_mentions"),
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("date_clean", "date")
        .orderBy("date", "avg_goldstein")
    )
    write_to_mongo(result, "conflict_heatmap")
    print("✔ Análisis 1 (heatmap de conflictos) completado.")


def run_conflict_pairs(events_df, spark):
    """
    Análisis 8: Pares de países que entran en conflicto con más frecuencia.
    QuadClass 3 = Conflicto verbal, 4 = Conflicto material.
    """
    result = (
        events_df
        .filter(F.col("QuadClass") >= 3)
        .filter(
            F.col("Actor1CountryCode").isNotNull() &
            F.col("Actor2CountryCode").isNotNull() &
            (F.col("Actor1CountryCode") != F.col("Actor2CountryCode"))
        )
        .groupBy("Actor1CountryCode", "Actor2CountryCode", "Day")
        .agg(
            F.count("GlobalEventID").alias("conflict_count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
        )
        .withColumnRenamed("Actor1CountryCode", "actor1_country")
        .withColumnRenamed("Actor2CountryCode", "actor2_country")
        .withColumnRenamed("Day", "date")
        .orderBy(F.desc("conflict_count"))
    )
    write_to_mongo(result, "conflict_pairs")
    print("✔ Análisis 8 (pares en conflicto) completado.")


def run_escalation_detection(events_df, mentions_df, spark):
    """
    Análisis 9: Detección de escalada — eventos con aumento acelerado de
    menciones en 24 horas. Calcula la diferencia de menciones entre
    ventanas temporales de 1h y 24h para el mismo GlobalEventID.
    """
    # Ventana de 1h y 24h de menciones por evento
    mentions_with_ts = (
        mentions_df
        .withColumn(
            "mention_hour",
            F.date_trunc("hour", F.to_timestamp(F.col("MentionTimeDate"), "yyyyMMddHHmmss"))
        )
        .groupBy("GlobalEventID", "mention_hour")
        .agg(F.count("*").alias("mentions_in_hour"))
    )

    window_24h = Window.partitionBy("GlobalEventID").orderBy("mention_hour").rowsBetween(-23, 0)
    window_1h  = Window.partitionBy("GlobalEventID").orderBy("mention_hour").rowsBetween(0, 0)

    escalation = (
        mentions_with_ts
        .withColumn("mentions_last_24h", F.sum("mentions_in_hour").over(window_24h))
        .withColumn("mentions_this_hour", F.sum("mentions_in_hour").over(window_1h))
        .withColumn(
            "escalation_ratio",
            F.col("mentions_this_hour") / (F.col("mentions_last_24h") + 1)
        )
        .filter(F.col("escalation_ratio") > 0.5)  # >50% del total en 1h = escalada
        .orderBy(F.desc("escalation_ratio"))
        .select("GlobalEventID", "mention_hour", "mentions_last_24h",
                "mentions_this_hour", "escalation_ratio")
    )
    write_to_mongo(escalation, "escalation_events")
    print("✔ Análisis 9 (escalada de eventos) completado.")


def run_diplomatic_network(events_df, spark):
    """
    Análisis 14: Red de interacciones diplomáticas vs. conflictos entre países.
    QuadClass 1-2 = verbal/material cooperación (diplomacia)
    QuadClass 3-4 = verbal/material conflicto
    """
    diplomatic = (
        events_df
        .filter(
            F.col("Actor1CountryCode").isNotNull() &
            F.col("Actor2CountryCode").isNotNull() &
            (F.col("Actor1CountryCode") != F.col("Actor2CountryCode"))
        )
        .groupBy("Actor1CountryCode", "Actor2CountryCode")
        .agg(
            F.sum(F.when(F.col("QuadClass") <= 2, 1).otherwise(0)).alias("diplomatic_count"),
            F.sum(F.when(F.col("QuadClass") >= 3, 1).otherwise(0)).alias("conflict_count"),
            F.count("GlobalEventID").alias("total_interactions"),
        )
        .withColumnRenamed("Actor1CountryCode", "source_country")
        .withColumnRenamed("Actor2CountryCode", "target_country")
        .orderBy(F.desc("total_interactions"))
    )
    write_to_mongo(diplomatic, "diplomatic_network")
    print("✔ Análisis 14 (red diplomática/conflicto) completado.")


def run_ethnic_conflicts(events_df, spark):
    """
    Análisis 16: Frecuencia de conflictos por etnia de los actores.
    Usa Actor1EthnicCode y Actor2EthnicCode del dataset de eventos.
    """
    actor1_ethnic = (
        events_df
        .filter(F.col("QuadClass") >= 3)
        .filter(F.col("Actor1EthnicCode").isNotNull() & (F.col("Actor1EthnicCode") != ""))
        .groupBy("Actor1EthnicCode", "Day")
        .agg(
            F.count("GlobalEventID").alias("event_count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
        )
        .withColumnRenamed("Actor1EthnicCode", "ethnicity")
        .withColumnRenamed("Day", "date")
    )

    actor2_ethnic = (
        events_df
        .filter(F.col("QuadClass") >= 3)
        .filter(F.col("Actor2EthnicCode").isNotNull() & (F.col("Actor2EthnicCode") != ""))
        .groupBy("Actor2EthnicCode", "Day")
        .agg(
            F.count("GlobalEventID").alias("event_count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
        )
        .withColumnRenamed("Actor2EthnicCode", "ethnicity")
        .withColumnRenamed("Day", "date")
    )

    result = (
        actor1_ethnic.union(actor2_ethnic)
        .groupBy("ethnicity", "date")
        .agg(
            F.sum("event_count").alias("event_count"),
            F.avg("avg_goldstein").alias("avg_goldstein"),
        )
        .orderBy(F.desc("event_count"))
    )
    write_to_mongo(result, "ethnic_conflicts")
    print("✔ Análisis 16 (conflictos por etnia) completado.")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = get_args()
    spark = create_session("GDELT-Conflicts", args.mongo_uri)
    spark.sparkContext.setLogLevel("WARN")

    print("Leyendo datos de Parquet…")
    events_df = read_events(spark, args.parquet_dir)
    mentions_df = spark.read.parquet(f"{args.parquet_dir}/mentions")

    # Cachear events ya que varios análisis lo usan
    events_df.cache()

    run_heatmap(events_df, spark)
    run_conflict_pairs(events_df, spark)
    run_escalation_detection(events_df, mentions_df, spark)
    run_diplomatic_network(events_df, spark)
    run_ethnic_conflicts(events_df, spark)

    spark.stop()
    print("═══ analysis_conflicts.py finalizado ═══")
