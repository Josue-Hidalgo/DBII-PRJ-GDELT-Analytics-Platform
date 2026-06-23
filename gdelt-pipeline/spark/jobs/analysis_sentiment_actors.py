"""
analysis_sentiment_actors.py — Sentimiento, Actores y GKG
===========================================================
Análisis incluidos:
  3.  Correlación AvgTone vs. número de fuentes noticiosas
  4.  Distribución de tipos de eventos CAMEO por región del mundo
  5.  Matriz de interacción entre tipos de actores
  7.  Tendencia de sentimiento por país (promedio móvil AvgTone, ventana 3 días)
 10.  Agrupamiento de eventos de conflicto por religión/región
 11.  Principales temas GKG por continente por año
 13.  Análisis de rezago: ¿el tono de hoy predice conflictos mañana?
 18.  [Extra] Top eventos con tono más extremo (positivo/negativo) del periodo
 19.  [Extra] Días de la semana con más actividad noticiosa

MongoDB schemas:
  tone_source_correlation   → {country, date, avg_tone, avg_sources, correlation}
  cameo_distribution        → {event_root_code, event_desc, region, date, event_count}
  actor_interaction_matrix  → {actor1_type, actor2_type, event_count, avg_goldstein}
  sentiment_trend           → {country, date, avg_tone, moving_avg_3d}
  religion_conflict_cluster → {religion, region, event_count, avg_goldstein, date}
  gkg_themes_continent      → {theme, continent, year, mention_count, rank}
  tone_conflict_lag         → {country, date, today_avg_tone, tomorrow_conflict_count}
  extreme_tone_events       → {GlobalEventID, country, EventRootCode, QuadClass, AvgTone, NumMentions, tone_extreme_type}
  weekday_activity          → {weekday, weekday_name, event_count}
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import create_session, get_args, write_to_mongo, read_events

# IMPORTANTE: GDELT usa códigos de país FIPS 10-4, NO ISO 3166-1 alpha-2.
# Algunos códigos que difieren entre ambos estándares (causa común de mapeos
# incorrectos si se usa una librería o tabla basada en ISO):
#   FIPS "UK" = Reino Unido   (ISO sería "GB")
#   FIPS "GM" = Alemania      (ISO "GM" no existe / en ISO sería Gambia)
#   FIPS "RS" = Rusia         (ISO "RS" es Serbia)
#   FIPS "SP" = España        (ISO sería "ES")
#   FIPS "PO" = Polonia       (ISO sería "PL")
#   FIPS "IZ" = Irak          (ISO sería "IQ")
#   FIPS "SF" = Sudáfrica     (ISO sería "ZA")
#   FIPS "CH" = China         (ISO sería "CN")
#   FIPS "JA" = Japón         (ISO sería "JP")
#   FIPS "KS" = Corea del Sur (ISO sería "KR")
#   FIPS "AS" = Australia     (ISO sería "AU")
#   FIPS "IS" = Israel        (ISO sería "IL")
#   FIPS "NI" = Nigeria       (ISO sería "NG")
#   FIPS "UP" = Ucrania       (ISO sería "UA")
# Todas las claves de REGION_MAP/CONTINENT_MAP/COUNTRY_NAMES deben ser FIPS.
REGION_MAP = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "BR": "South America", "AR": "South America", "CO": "South America",
    "UK": "Europe", "FR": "Europe", "GM": "Europe", "IT": "Europe", "SP": "Europe",
    "RS": "Europe/Asia", "UP": "Europe", "PO": "Europe",
    "CH": "Asia", "JA": "Asia", "IN": "Asia", "KS": "Asia", "PK": "Asia",
    "IR": "Middle East", "IZ": "Middle East", "SA": "Middle East", "IS": "Middle East",
    "NI": "Africa", "ET": "Africa", "KE": "Africa", "SF": "Africa", "EG": "Africa",
    "AS": "Oceania", "NZ": "Oceania",
}

CONTINENT_MAP = {
    "US": "Americas", "CA": "Americas", "MX": "Americas", "BR": "Americas",
    "UK": "Europe", "FR": "Europe", "GM": "Europe", "RS": "Europe",
    "CH": "Asia", "IN": "Asia", "JA": "Asia",
    "NI": "Africa", "SF": "Africa", "EG": "Africa",
    "AS": "Oceania",
}

# Nombres legibles FIPS → nombre completo, para usar en el dashboard
# (evita mostrar solo el código de 2 letras en tablas/gráficas).
COUNTRY_NAMES = {
    "US": "Estados Unidos", "CA": "Canadá", "MX": "México",
    "BR": "Brasil", "AR": "Argentina", "CO": "Colombia",
    "UK": "Reino Unido", "FR": "Francia", "GM": "Alemania", "IT": "Italia", "SP": "España",
    "RS": "Rusia", "UP": "Ucrania", "PO": "Polonia",
    "CH": "China", "JA": "Japón", "IN": "India", "KS": "Corea del Sur", "PK": "Pakistán",
    "IR": "Irán", "IZ": "Irak", "SA": "Arabia Saudita", "IS": "Israel",
    "NI": "Nigeria", "ET": "Etiopía", "KE": "Kenia", "SF": "Sudáfrica", "EG": "Egipto",
    "AS": "Australia", "NZ": "Nueva Zelanda",
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
    Análisis 7: Tendencia de sentimiento por país con promedio móvil.
    Usamos ventana de 3 días (no 7) porque el loader solo retiene
    RAW_RETENTION_HOURS (ver .env) y agrupamos por día (date_clean = YYYYMMDD).
    Una ventana de 7 días rara vez tendría suficientes días reales acumulados
    para mostrar una tendencia visible; 3 días sigue demostrando el
    promedio móvil con los datos realmente disponibles en el pipeline.
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

    # Promedio móvil de 3 días usando window function
    window_3d = (
        Window
        .partitionBy("country")
        .orderBy(F.col("date"))
        .rowsBetween(-2, 0)
    )

    result = (
        daily_tone
        .withColumn("moving_avg_3d", F.avg("avg_tone").over(window_3d))
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

def run_extreme_tone_events(events_df, spark):
    """
    Análisis 18 [Extra]: Top eventos con el tono más extremo del periodo,
    tanto positivo (máxima cooperación percibida) como negativo (máxima
    hostilidad percibida) según AvgTone.
    Reemplaza el análisis original de "polarización semanal" porque ese
    agrupaba por semana del año y, con pocas horas de datos RAW retenidos
    por el loader, casi siempre cae en una sola semana — la gráfica no
    tiene nada que mostrar. Este análisis sí se puebla siempre que haya
    al menos un puñado de eventos, sin depender de acumular varias semanas.
    """
    base = (
        events_df
        .filter(F.col("AvgTone").isNotNull() & F.col("ActionGeo_CountryCode").isNotNull())
        .select(
            "GlobalEventID", "ActionGeo_CountryCode", "ActionGeo_FullName",
            "EventRootCode", "QuadClass", "AvgTone", "NumMentions", "Day",
        )
        .withColumnRenamed("ActionGeo_CountryCode", "country")
    )

    most_negative = (
        base.orderBy(F.col("AvgTone").asc())
        .limit(15)
        .withColumn("tone_extreme_type", F.lit("most_negative"))
    )
    most_positive = (
        base.orderBy(F.col("AvgTone").desc())
        .limit(15)
        .withColumn("tone_extreme_type", F.lit("most_positive"))
    )

    result = most_negative.unionByName(most_positive).orderBy(F.desc("tone_extreme_type"), F.col("AvgTone").asc())
    write_to_mongo(result, "extreme_tone_events")
    print("✔ Análisis 18 (eventos con tono más extremo) completado.")


def run_weekday_activity(events_df, spark):
    """
    Análisis 19 [Extra]: Días de la semana con más actividad noticiosa.
    Extrae el día de la semana de cada evento, cuenta eventos y agrupa por día.
    """
    weekday_names = {
        1: "Lunes",
        2: "Martes",
        3: "Miércoles",
        4: "Jueves",
        5: "Viernes",
        6: "Sábado",
        7: "Domingo"
    }

    weekday_map_df = spark.createDataFrame(
        [(k, v) for k, v in weekday_names.items()],
        ["weekday_num", "weekday_name"]
    )

    result = (
        events_df
        .withColumn("event_ts", F.to_timestamp(F.col("DATEADDED"), "yyyyMMddHHmmss"))
        .filter(F.col("event_ts").isNotNull())
        .withColumn("weekday_num", F.dayofweek(F.col("event_ts")))
        .groupBy("weekday_num")
        .agg(F.count("GlobalEventID").alias("event_count"))
        .join(weekday_map_df, on="weekday_num", how="inner")
        .select("weekday_num", "weekday_name", "event_count")
        .orderBy("weekday_num")
    )
    write_to_mongo(result, "weekday_activity")
    print("✔ Análisis 19 (actividad por día de la semana) completado.")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = get_args()
    spark = create_session("GDELT-Sentiment-Actors", args.mongo_uri)
    spark.sparkContext.setLogLevel("WARN")

    events_df = read_events(spark, args.parquet_dir).cache()
    gkg_df      = spark.read.parquet(f"{args.parquet_dir}/gkg")
    mentions_df = spark.read.parquet(f"{args.parquet_dir}/mentions")

    run_tone_source_correlation(events_df, spark)
    run_cameo_distribution(events_df, spark)
    run_actor_interaction_matrix(events_df, spark)
    run_sentiment_trend(events_df, spark)
    run_religion_conflict_cluster(events_df, spark)
    run_gkg_themes_continent(gkg_df, spark)
    run_tone_conflict_lag(events_df, spark)
    run_extreme_tone_events(events_df, spark)
    run_weekday_activity(events_df, spark)

    spark.stop()
    print("═══ analysis_sentiment_actors.py finalizado ═══")
