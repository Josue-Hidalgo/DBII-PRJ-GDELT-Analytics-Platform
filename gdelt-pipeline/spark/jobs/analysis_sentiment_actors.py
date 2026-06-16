"""
analysis_sentiment_actors.py — Sentimiento, Actores y GKG
===========================================================
Análisis incluidos:
  3.  Correlación AvgTone vs. número de fuentes noticiosas
  4.  Distribución de tipos de eventos CAMEO por región del mundo
  5.  Matriz de interacción entre tipos de actores
  7.  Tendencia de sentimiento por país (promedio móvil AvgTone)
 10.  Agrupamiento de eventos de conflicto por religión/región
 11.  Principales temas GKG por continente por año
 13.  Análisis de rezago: ¿el tono de hoy predice conflictos mañana?
 18.  [Extra] Índice de polarización mediática por país/semana
 19.  [Extra] Eventos de cooperación entre países en conflicto crónico

MongoDB schemas:
  tone_source_correlation   → {country, date, avg_tone, avg_sources, correlation}
  cameo_distribution        → {event_root_code, event_desc, region, date, event_count}
  actor_interaction_matrix  → {actor1_type, actor2_type, event_count, avg_goldstein}
  sentiment_trend           → {country, date, avg_tone, moving_avg_7d}
  religion_conflict_cluster → {religion, region, event_count, avg_goldstein, date}
  gkg_themes_continent      → {theme, continent, year, mention_count, rank}
  tone_conflict_lag         → {country, date, today_avg_tone, tomorrow_conflict_count}
  media_polarization        → {country, week, tone_std_dev, polarization_index}
  cooperation_amid_conflict  → {actor1_country, actor2_country, conflict_events, cooperation_events, ratio}
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import create_session, get_args, write_to_mongo, read_events

# Mapeo de código de país a región (simplificado — en producción usar un lookup completo)
REGION_MAP = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "BR": "South America", "AR": "South America", "CO": "South America",
    "GB": "Europe", "FR": "Europe", "DE": "Europe", "IT": "Europe", "ES": "Europe",
    "RU": "Europe/Asia", "UA": "Europe",
    "CN": "Asia", "JP": "Asia", "IN": "Asia", "KR": "Asia", "PK": "Asia",
    "IR": "Middle East", "IQ": "Middle East", "SA": "Middle East", "IL": "Middle East",
    "NG": "Africa", "ET": "Africa", "KE": "Africa", "ZA": "Africa", "EG": "Africa",
    "AU": "Oceania", "NZ": "Oceania",
}

CONTINENT_MAP = {
    "US": "Americas", "CA": "Americas", "MX": "Americas", "BR": "Americas",
    "GB": "Europe", "FR": "Europe", "DE": "Europe", "RU": "Europe",
    "CN": "Asia", "IN": "Asia", "JP": "Asia",
    "NG": "Africa", "ZA": "Africa", "EG": "Africa",
    "AU": "Oceania",
}


def run_tone_source_correlation(events_df, spark):
    """
    Análisis 3: Correlación entre AvgTone y número de fuentes (NumSources).
    Calcula Pearson correlation por país y día.
    """
    result = (
        events_df
        .filter(
            F.col("AvgTone").isNotNull() &
            F.col("NumSources").isNotNull() &
            F.col("ActionGeo_CountryCode").isNotNull()
        )
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("ActionGeo_CountryCode", "date_clean")
        .agg(
            F.avg(F.col("AvgTone").cast("double")).alias("avg_tone"),
            F.avg(F.col("NumSources").cast("double")).alias("avg_sources"),
            F.corr(F.col("AvgTone").cast("double"), F.col("NumSources").cast("double")).alias("tone_sources_correlation"),
            F.count("GlobalEventID").alias("sample_size"),
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("date_clean", "date")
        .orderBy(F.desc("sample_size"))
    )
    write_to_mongo(result, "tone_source_correlation")
    print("✔ Análisis 3 (correlación tono/fuentes) completado.")


def run_cameo_distribution(events_df, spark):
    """
    Análisis 4: Distribución de tipos de eventos CAMEO por región.
    EventRootCode tiene los 20 tipos base del sistema CAMEO.
    """
    # Lookup básico de códigos CAMEO raíz
    cameo_root_labels = {
        "01": "Declaraciones públicas",
        "02": "Apelaciones",
        "03": "Expresar intención",
        "04": "Consultar",
        "05": "Diplomacia",
        "06": "Cooperar materialmente",
        "07": "Dar ayuda",
        "08": "Ceder",
        "09": "Investigar",
        "10": "Exigir",
        "11": "Rechazar",
        "12": "Acusar",
        "13": "Amenazar",
        "14": "Protestas",
        "15": "Obstruir",
        "16": "Coerción",
        "17": "Asalto",
        "18": "Ataque",
        "19": "Violencia masiva",
        "20": "Uso de armas no convencionales",
    }

    # Crear una lookup table en Spark
    cameo_df = spark.createDataFrame(
        [(k, v) for k, v in cameo_root_labels.items()],
        ["EventRootCode", "event_description"]
    )

    region_map_df = spark.createDataFrame(
        [(k, v) for k, v in REGION_MAP.items()],
        ["ActionGeo_CountryCode", "region"]
    )

    result = (
        events_df
        .filter(F.col("EventRootCode").isNotNull())
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .join(region_map_df, on="ActionGeo_CountryCode", how="left")
        .fillna({"region": "Other"})
        .join(cameo_df, on="EventRootCode", how="left")
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("EventRootCode", "event_description", "region", "date_clean")
        .agg(F.count("GlobalEventID").alias("event_count"))
        .withColumnRenamed("date_clean", "date")
        .orderBy("date", "region", F.desc("event_count"))
    )
    write_to_mongo(result, "cameo_distribution")
    print("✔ Análisis 4 (distribución CAMEO por región) completado.")


def run_actor_interaction_matrix(events_df, spark):
    """
    Análisis 5: Matriz de interacción entre tipos de actores.
    Agrupa por Actor1Type1Code vs Actor2Type1Code.
    Tipos relevantes: GOV=Gobierno, MIL=Militar, REB=Rebeldes, etc.
    """
    result = (
        events_df
        .filter(
            F.col("Actor1Type1Code").isNotNull() &
            F.col("Actor2Type1Code").isNotNull()
        )
        .groupBy("Actor1Type1Code", "Actor2Type1Code")
        .agg(
            F.count("GlobalEventID").alias("event_count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
            F.avg("AvgTone").alias("avg_tone"),
        )
        .withColumnRenamed("Actor1Type1Code", "actor1_type")
        .withColumnRenamed("Actor2Type1Code", "actor2_type")
        .orderBy(F.desc("event_count"))
    )
    write_to_mongo(result, "actor_interaction_matrix")
    print("✔ Análisis 5 (matriz de interacción actores) completado.")


def run_sentiment_trend(events_df, spark):
    """
    Análisis 7: Tendencia de sentimiento por país con promedio móvil de 7 días.
    """
    daily_tone = (
        events_df
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("ActionGeo_CountryCode", "date_clean")
        .agg(F.avg(F.col("AvgTone").cast("double")).alias("avg_tone"))
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("date_clean", "date")
        .orderBy("country", "date")
    )

    # Promedio móvil de 7 días usando window function
    window_7d = (
        Window
        .partitionBy("country")
        .orderBy(F.col("date"))
        .rowsBetween(-6, 0)
    )

    result = (
        daily_tone
        .withColumn("moving_avg_7d", F.avg("avg_tone").over(window_7d))
        .orderBy("country", "date")
    )
    write_to_mongo(result, "sentiment_trend")
    print("✔ Análisis 7 (tendencia de sentimiento) completado.")


def run_religion_conflict_cluster(events_df, spark):
    """
    Análisis 10: Agrupamiento de eventos de conflicto por religión y región.
    Usa Actor1Religion1Code y Actor2Religion1Code.
    """
    region_map_df = spark.createDataFrame(
        [(k, v) for k, v in REGION_MAP.items()],
        ["ActionGeo_CountryCode", "region"]
    )

    result = (
        events_df
        .filter(F.col("QuadClass") >= 3)  # Solo conflictos
        .filter(
            F.col("Actor1Religion1Code").isNotNull() |
            F.col("Actor2Religion1Code").isNotNull()
        )
        .withColumn(
            "religion",
            F.coalesce(F.col("Actor1Religion1Code"), F.col("Actor2Religion1Code"))
        )
        .filter(F.col("religion") != "")
        .join(region_map_df, on="ActionGeo_CountryCode", how="left")
        .fillna({"region": "Other"})
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("religion", "region", "date_clean")
        .agg(
            F.count("GlobalEventID").alias("event_count"),
            F.avg(F.col("GoldsteinScale").cast("double")).alias("avg_goldstein"),
        )
        .withColumnRenamed("date_clean", "date")
        .orderBy(F.desc("event_count"))
    )
    write_to_mongo(result, "religion_conflict_cluster")
    print("✔ Análisis 10 (conflictos por religión/región) completado.")


def run_gkg_themes_continent(gkg_df, spark):
    """
    Análisis 11: Principales temas del GKG por continente y año.
    Parsea el campo V2Themes (separado por ';') y cruza con continente.
    """
    continent_map_df = spark.createDataFrame(
        [(k, v) for k, v in CONTINENT_MAP.items()],
        ["country_code", "continent"]
    )

    themes_df = (
        gkg_df
        .select("DATE", "V2Locations", "V2Themes")
        .filter(F.col("V2Themes").isNotNull() & (F.col("V2Themes") != ""))
        .withColumn("year", F.substring(F.col("DATE"), 1, 4))
        # Extraer el primer código de país de V2Locations (formato: type#name#cc#...)
        .withColumn(
            "country_code",
            F.regexp_extract(F.col("V2Locations"), r"#([A-Z]{2})#", 1)
        )
        .withColumn("theme_raw", F.explode(F.split(F.col("V2Themes"), ";")))
        # Tomar solo el nombre del tema (antes del primer '#')
        .withColumn("theme", F.split(F.split(F.col("theme_raw"), "#").getItem(0), ",").getItem(0))
        .filter(F.col("theme") != "")
    )

    window = Window.partitionBy("continent", "year").orderBy(F.desc("mention_count"))

    result = (
        themes_df
        .join(continent_map_df, on="country_code", how="left")
        .fillna({"continent": "Unknown"})
        .groupBy("theme", "continent", "year")
        .agg(F.count("*").alias("mention_count"))
        .withColumn("rank", F.rank().over(window))
        .filter(F.col("rank") <= 10)  # Top 10 temas por continente/año
        .orderBy("continent", "year", "rank")
    )
    write_to_mongo(result, "gkg_themes_continent")
    print("✔ Análisis 11 (temas GKG por continente) completado.")


def run_tone_conflict_lag(events_df, spark):
    daily = (
        events_df
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        .withColumn("date_clean", F.substring(F.col("DATEADDED").cast("string"), 1, 8).cast("int"))
        .groupBy("ActionGeo_CountryCode", "date_clean")
        .agg(
            F.avg(F.col("AvgTone").cast("double")).alias("today_avg_tone"),
            F.sum(F.when(F.col("QuadClass").cast("int") >= 3, 1).otherwise(0)).alias("today_conflict_count"),
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .withColumnRenamed("date_clean", "date")
    )

    daily_lag = daily.select(
        F.col("country"),
        (F.col("date") - 1).alias("prev_date"),
        F.col("today_conflict_count").alias("tomorrow_conflict_count"),
    )

    result = (
        daily.alias("today")
        .join(
            daily_lag.alias("tomorrow"),
            on=[
                F.col("today.country") == F.col("tomorrow.country"),
                F.col("today.date") == F.col("tomorrow.prev_date"),
            ],
            how="left"
        )
        .select(
            F.col("today.country"),
            F.col("today.date"),
            F.col("today.today_avg_tone"),
            F.col("today.today_conflict_count"),
            F.col("tomorrow.tomorrow_conflict_count"),
        )
        .orderBy("country", "date")
    )
    write_to_mongo(result, "tone_conflict_lag")
    print("✔ Análisis 13 (rezago tono→conflicto) completado.")

def run_media_polarization(events_df, spark):
    result = (
        events_df
        .filter(F.col("ActionGeo_CountryCode").isNotNull())
        # Usar Day (ya es int YYYYMMDD) en vez de parsear DATEADDED
        .withColumn("date_str", F.col("Day").cast("string"))
        .withColumn("date_clean", F.to_date(F.col("date_str"), "yyyyMMdd"))
        .filter(F.col("date_clean").isNotNull())
        .withColumn("week", F.weekofyear(F.col("date_clean")))
        .withColumn("year", F.year(F.col("date_clean")))
        .groupBy("ActionGeo_CountryCode", "year", "week")
        .agg(
            F.stddev(F.col("AvgTone").cast("double")).alias("tone_std_dev"),
            F.avg(F.col("AvgTone").cast("double")).alias("avg_tone"),
            F.count("GlobalEventID").alias("event_count"),
        )
        .withColumn(
            "polarization_index",
            F.col("tone_std_dev") / (F.abs(F.col("avg_tone")) + 1)
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
        .orderBy(F.desc("polarization_index"))
    )
    write_to_mongo(result, "media_polarization")
    print("✔ Análisis 18 (polarización mediática) completado.")

def run_cooperation_amid_conflict(events_df, spark):
    pairs = (
        events_df
        .filter(
            F.col("Actor1CountryCode").isNotNull() &
            F.col("Actor2CountryCode").isNotNull() &
            (F.col("Actor1CountryCode") != F.col("Actor2CountryCode"))
        )
        .groupBy("Actor1CountryCode", "Actor2CountryCode")
        .agg(
            F.sum(F.when(F.col("QuadClass").cast("int") >= 3, 1).otherwise(0)).alias("conflict_events"),
            F.sum(F.when(F.col("QuadClass").cast("int") <= 2, 1).otherwise(0)).alias("cooperation_events"),
            F.count("GlobalEventID").alias("total_events"),
        )
        .withColumn(
            "cooperation_ratio",
            F.col("cooperation_events") / (F.col("total_events") + 1)
        )
        .filter(F.col("conflict_events") > 20)
        .withColumnRenamed("Actor1CountryCode", "actor1_country")
        .withColumnRenamed("Actor2CountryCode", "actor2_country")
        .orderBy(F.desc("cooperation_ratio"))
    )
    write_to_mongo(pairs, "cooperation_amid_conflict")
    print("✔ Análisis 19 (cooperación entre países en conflicto) completado.")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = get_args()
    spark = create_session("GDELT-Sentiment-Actors", args.mongo_uri)
    spark.sparkContext.setLogLevel("WARN")

    events_df = read_events(spark, args.parquet_dir).cache()
    gkg_df      = spark.read.parquet(f"{args.parquet_dir}/gkg")

    run_tone_source_correlation(events_df, spark)
    run_cameo_distribution(events_df, spark)
    run_actor_interaction_matrix(events_df, spark)
    run_sentiment_trend(events_df, spark)
    run_religion_conflict_cluster(events_df, spark)
    run_gkg_themes_continent(gkg_df, spark)
    run_tone_conflict_lag(events_df, spark)
    run_media_polarization(events_df, spark)
    run_cooperation_amid_conflict(events_df, spark)

    spark.stop()
    print("═══ analysis_sentiment_actors.py finalizado ═══")
