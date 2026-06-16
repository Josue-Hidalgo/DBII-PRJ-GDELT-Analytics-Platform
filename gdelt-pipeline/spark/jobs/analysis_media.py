"""
analysis_media.py — Análisis de Cobertura Mediática
=====================================================
Análisis incluidos:
  2.  Top 10 países que generan más eventos noticiosos por día
  6.  Países con mayor cobertura mediática por evento (razón menciones/evento)
 12.  Organizaciones más mencionadas a nivel global por día
 15.  Índice de diversidad de fuentes por país
 17.  Detección de noticias de última hora (0 → >100 menciones en <1h)

MongoDB schemas:
  top_news_countries     → {country, date, event_count, rank}
  media_coverage_ratio   → {country, date, mentions_per_event, total_mentions, total_events}
  top_organizations      → {organization, date, mention_count, rank}
  source_diversity_index → {country, date, unique_sources, source_diversity_index}
  breaking_news          → {event_id, first_mention_time, time_to_100_mentions_minutes, country}
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# línea 1 — import
from spark_session import create_session, get_args, write_to_mongo, read_events


def run_top_news_countries(events_df, spark):
    """
    Análisis 2: Top 10 países que generan más eventos noticiosos por día.
    Usa ActionGeo_CountryCode como país del evento.
    """
    window = Window.partitionBy("date").orderBy(F.desc("event_count"))

    result = (
        events_df
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("ActionGeo_CountryCode", "date_clean")
        .agg(F.count("GlobalEventID").alias("event_count"))
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("date_clean", "date")
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 10)
        .orderBy("date", "rank")
    )
    write_to_mongo(result, "top_news_countries")
    print("✔ Análisis 2 (top países noticiosos) completado.")


def run_media_coverage_ratio(events_df, spark):
    """
    Análisis 6: Razón menciones/evento por país — qué países tienen más
    cobertura mediática relativa a la cantidad de eventos que generan.
    """
    result = (
        events_df
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .groupBy("ActionGeo_CountryCode", "Day")
        .agg(
            F.sum("NumMentions").alias("total_mentions"),
            F.count("GlobalEventID").alias("total_events"),
        )
        .withColumn(
            "mentions_per_event",
            F.col("total_mentions") / F.col("total_events")
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("Day", "date")
        .orderBy(F.desc("mentions_per_event"))
    )
    write_to_mongo(result, "media_coverage_ratio")
    print("✔ Análisis 6 (razón cobertura mediática) completado.")


def run_top_organizations(gkg_df, spark):
    """
    Análisis 12: Organizaciones más mencionadas a nivel global por día.
    Parsea el campo Organizations del GKG (separado por ';').
    """
    # Explotar el campo Organizations (lista separada por ';')
    orgs_df = (
        gkg_df
        .select("DATE", "Organizations")
        .filter(F.col("Organizations").isNotNull() & (F.col("Organizations") != ""))
        .withColumn("organization", F.explode(F.split(F.col("Organizations"), ";")))
        .withColumn("organization", F.trim(F.col("organization")))
        .filter(F.col("organization") != "")
        .withColumn("date", F.substring(F.col("DATE"), 1, 8))  # YYYYMMDD
    )

    window = Window.partitionBy("date").orderBy(F.desc("mention_count"))

    result = (
        orgs_df
        .groupBy("organization", "date")
        .agg(F.count("*").alias("mention_count"))
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 20)  # Top 20 por día
        .orderBy("date", "rank")
    )
    write_to_mongo(result, "top_organizations")
    print("✔ Análisis 12 (top organizaciones) completado.")


def run_source_diversity_index(mentions_df, events_df, spark):
    joined = (
        mentions_df
        .join(
            events_df.select("GlobalEventID", "ActionGeo_CountryCode", F.col("Day").alias("EventDay")),
            on="GlobalEventID",
            how="inner"
        )
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .filter(F.col("MentionSourceName").isNotNull())
    )

    result = (
        joined
        .groupBy("ActionGeo_CountryCode", "EventDay")
        .agg(
            F.countDistinct("MentionSourceName").alias("unique_sources"),
            F.count("*").alias("total_mentions"),
        )
        .withColumn("source_diversity_index", F.log(F.col("unique_sources") + 1))
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("EventDay", "date")
        .orderBy(F.desc("source_diversity_index"))
    )
    write_to_mongo(result, "source_diversity_index")
    print("✔ Análisis 15 (índice diversidad de fuentes) completado.")



def run_breaking_news(mentions_df, events_df, spark):
    """
    Análisis 17: Detección de noticias de última hora.
    Eventos que pasan de 0 a >100 menciones en menos de 1 hora.
    """
    mentions_parsed = (
        mentions_df
        .withColumn(
            "mention_ts",
            F.to_timestamp(F.col("MentionTimeDate"), "yyyyMMddHHmmss")
        )
        .filter(F.col("mention_ts").isNotNull())
    )

    # Primera mención de cada evento
    first_mention = (
        mentions_parsed
        .groupBy("GlobalEventID")
        .agg(F.min("mention_ts").alias("first_mention_time"))
    )

    # Menciones dentro de 1h del primer avistamiento
    within_hour = (
        mentions_parsed
        .join(first_mention, on="GlobalEventID")
        .filter(
            F.col("mention_ts") <= F.col("first_mention_time") + F.expr("INTERVAL 1 HOUR")
        )
        .groupBy("GlobalEventID", "first_mention_time")
        .agg(F.count("*").alias("mentions_in_first_hour"))
        .filter(F.col("mentions_in_first_hour") >= 100)
    )

    result = (
        within_hour
        .join(
            events_df.select("GlobalEventID", "ActionGeo_CountryCode", "Day"),
            on="GlobalEventID",
            how="left"
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("Day", "date")
        .orderBy(F.desc("mentions_in_first_hour"))
    )
    write_to_mongo(result, "breaking_news")
    print("✔ Análisis 17 (noticias de última hora) completado.")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = get_args()
    spark = create_session("GDELT-Media", args.mongo_uri)
    spark.sparkContext.setLogLevel("WARN")

    events_df = read_events(spark, args.parquet_dir).cache()
    mentions_df = spark.read.parquet(f"{args.parquet_dir}/mentions").cache()
    gkg_df      = spark.read.parquet(f"{args.parquet_dir}/gkg")

    run_top_news_countries(events_df, spark)
    run_media_coverage_ratio(events_df, spark)
    run_top_organizations(gkg_df, spark)
    run_source_diversity_index(mentions_df, events_df, spark)
    run_breaking_news(mentions_df, events_df, spark)

    spark.stop()
    print("═══ analysis_media.py finalizado ═══")
