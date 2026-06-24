"""
loader.py — GDELT Data Loader
==============================
Descarga los archivos GDELT de los últimos 15 minutos (Events, Mentions, GKG),
los convierte a formato Parquet y limpia los datos RAW con más de N horas de antigüedad.

Puede correr en loop autónomo (schedule) o ser invocado por Airflow como un script externo.

Variables de entorno:
  LOADER_INTERVAL_MINUTES  — cada cuántos minutos correr (default: 15)
  RAW_RETENTION_HOURS      — cuánto tiempo guardar los RAW (default: 1)
  PARQUET_OUTPUT_DIR       — ruta de salida de los Parquet (default: /data/parquet)
  LOG_LEVEL                — nivel de logging (default: INFO)
"""

import io
import logging
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import schedule

EVENTS_ARROW_SCHEMA = pa.schema([
    pa.field("GlobalEventID", pa.int32()),
    pa.field("Day", pa.int32()),
    pa.field("MonthYear", pa.int32()),
    pa.field("Year", pa.int32()),
    pa.field("FractionDate", pa.float64()),
    pa.field("Actor1Code", pa.string()),
    pa.field("Actor1Name", pa.string()),
    pa.field("Actor1CountryCode", pa.string()),
    pa.field("Actor1KnownGroupCode", pa.string()),
    pa.field("Actor1EthnicCode", pa.string()),
    pa.field("Actor1Religion1Code", pa.string()),
    pa.field("Actor1Religion2Code", pa.string()),
    pa.field("Actor1Type1Code", pa.string()),
    pa.field("Actor1Type2Code", pa.string()),
    pa.field("Actor1Type3Code", pa.string()),
    pa.field("Actor2Code", pa.string()),
    pa.field("Actor2Name", pa.string()),
    pa.field("Actor2CountryCode", pa.string()),
    pa.field("Actor2KnownGroupCode", pa.string()),
    pa.field("Actor2EthnicCode", pa.string()),
    pa.field("Actor2Religion1Code", pa.string()),
    pa.field("Actor2Religion2Code", pa.string()),
    pa.field("Actor2Type1Code", pa.string()),
    pa.field("Actor2Type2Code", pa.string()),
    pa.field("Actor2Type3Code", pa.string()),
    pa.field("IsRootEvent", pa.int32()),
    pa.field("EventCode", pa.string()),
    pa.field("EventBaseCode", pa.string()),
    pa.field("EventRootCode", pa.string()),
    pa.field("QuadClass", pa.int32()),
    pa.field("GoldsteinScale", pa.float64()),
    pa.field("NumMentions", pa.int32()),
    pa.field("NumSources", pa.int32()),
    pa.field("NumArticles", pa.int32()),
    pa.field("AvgTone", pa.float64()),
    pa.field("Actor1Geo_Type", pa.string()),
    pa.field("Actor1Geo_FullName", pa.string()),
    pa.field("Actor1Geo_CountryCode", pa.string()),
    pa.field("Actor1Geo_ADM1Code", pa.string()),
    pa.field("Actor1Geo_ADM2Code", pa.string()),
    pa.field("Actor1Geo_Lat", pa.float64()),
    pa.field("Actor1Geo_Long", pa.float64()),
    pa.field("Actor1Geo_FeatureID", pa.string()),
    pa.field("Actor2Geo_Type", pa.string()),
    pa.field("Actor2Geo_FullName", pa.string()),
    pa.field("Actor2Geo_CountryCode", pa.string()),
    pa.field("Actor2Geo_ADM1Code", pa.string()),
    pa.field("Actor2Geo_ADM2Code", pa.string()),
    pa.field("Actor2Geo_Lat", pa.float64()),
    pa.field("Actor2Geo_Long", pa.float64()),
    pa.field("Actor2Geo_FeatureID", pa.string()),
    pa.field("ActionGeo_Type", pa.string()),
    pa.field("ActionGeo_FullName", pa.string()),
    pa.field("ActionGeo_CountryCode", pa.string()),
    pa.field("ActionGeo_ADM1Code", pa.string()),
    pa.field("ActionGeo_ADM2Code", pa.string()),
    pa.field("ActionGeo_Lat", pa.float64()),
    pa.field("ActionGeo_Long", pa.float64()),
    pa.field("ActionGeo_FeatureID", pa.string()),
    pa.field("DATEADDED", pa.string()),
    pa.field("SOURCEURL", pa.string()),
])
# ─── Configuración ────────────────────────────────────────────────────────────

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
INTERVAL_MINUTES = int(os.getenv("LOADER_INTERVAL_MINUTES", 15))
RETENTION_HOURS = int(os.getenv("RAW_RETENTION_HOURS", 1))
PARQUET_DIR = Path(os.getenv("PARQUET_OUTPUT_DIR", "/data/parquet"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Tipos de archivos GDELT que nos interesan
GDELT_FILE_TYPES = {"export": "events", "mentions": "mentions", "gkg": "gkg"}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gdelt-loader")


# ─── Schemas de columnas GDELT (simplificados) ────────────────────────────────

EVENTS_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
    "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat",
    "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL",
]

MENTIONS_COLUMNS = [
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "MentionSourceName", "MentionIdentifier", "SentenceID",
    "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset",
    "InRawText", "Confidence", "MentionDocLen", "MentionDocTone",
    "MentionDocTranslationInfo", "Extras",
]

GKG_COLUMNS = [
    "GKGRECORDID", "DATE", "SourceCollectionIdentifier", "SourceCommonName",
    "DocumentIdentifier", "Counts", "V2Counts", "Themes", "V2Themes",
    "Locations", "V2Locations", "Persons", "V2Persons", "Organizations",
    "V2Organizations", "V2Tone", "Dates", "GCAM", "SharingImage",
    "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds", "Quotations",
    "AllNames", "Amounts", "TranslationInfo", "Extras",
]

TABLE_SCHEMAS = {
    "events": EVENTS_COLUMNS,
    "mentions": MENTIONS_COLUMNS,
    "gkg": GKG_COLUMNS,
}


# ─── Funciones principales ────────────────────────────────────────────────────

def fetch_lastupdate() -> list[dict]:
    """
    Descarga el archivo lastupdate.txt de GDELT y extrae las URLs
    de los tres tipos de archivo (export/events, mentions, gkg).

    Retorna una lista de dicts: [{"type": "events", "url": "...", "md5": "..."}]
    """
    logger.info("Obteniendo lista de últimas actualizaciones de GDELT…")
    try:
        resp = requests.get(LASTUPDATE_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error al descargar lastupdate.txt: %s", exc)
        return []

    files = []
    for line in resp.text.strip().splitlines():
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        size, md5, url = parts[0], parts[1], parts[2]
        # Identificar el tipo de tabla a partir del nombre del archivo
        for key, table_name in GDELT_FILE_TYPES.items():
            if f".{key}." in url.lower() or url.lower().endswith(f"{key}.csv.zip"):
                files.append({"type": table_name, "url": url, "md5": md5})
                break

    logger.info("Archivos encontrados: %s", [f["type"] for f in files])
    return files


def download_and_parse(file_info: dict) -> pd.DataFrame | None:
    """
    Descarga un ZIP de GDELT, lo descomprime en memoria y carga el CSV
    en un DataFrame de pandas con las columnas apropiadas.
    """
    url = file_info["url"]
    table_type = file_info["type"]
    columns = TABLE_SCHEMAS.get(table_type)

    logger.info("Descargando %s desde %s", table_type, url)
    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error al descargar %s: %s", url, exc)
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as csv_file:
                df = pd.read_csv(
                    csv_file,
                    sep="\t",
                    header=None,
                    names=columns,
                    dtype=str,       # Leemos todo como str para evitar errores de tipo
                    low_memory=False,
                    on_bad_lines="skip",
                )
        logger.info("Filas cargadas para %s: %d", table_type, len(df))
        return df
    except Exception as exc:
        logger.error("Error al parsear %s: %s", table_type, exc)
        return None


def cast_numeric_columns(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    numeric_map = {
        "events": {
            "float": ["GlobalEventID", "Day", "MonthYear", "Year", "NumMentions",
                      "NumSources", "NumArticles", "IsRootEvent", "QuadClass",
                      "GoldsteinScale", "AvgTone", "Actor1Geo_Lat", "Actor1Geo_Long",
                      "Actor2Geo_Lat", "Actor2Geo_Long", "ActionGeo_Lat", "ActionGeo_Long",
                      "FractionDate"],
            "int": [],
        },
        "mentions": {
            "float": ["GlobalEventID", "MentionType", "Confidence", "MentionDocLen",
                      "SentenceID", "InRawText", "MentionDocTone"],
            "int": [],
        },
        "gkg": {},
    }
    spec = numeric_map.get(table_type, {})
    for col in spec.get("float", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def save_parquet(df: pd.DataFrame, table_type: str, timestamp: datetime) -> Path | None:
    """
    Guarda el DataFrame en formato Parquet particionado por año/mes/día/hora.
    Estructura: PARQUET_DIR/<table>/<year>/<month>/<day>/<hour>/data.parquet
    """
    part_dir = (
        PARQUET_DIR
        / table_type
        / f"year={timestamp.year}"
        / f"month={timestamp.month:02d}"
        / f"day={timestamp.day:02d}"
        / f"hour={timestamp.hour:02d}"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    out_path = part_dir / f"{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"

    try:
        
        if table_type == "events":
            table = pa.Table.from_pandas(df, schema=EVENTS_ARROW_SCHEMA, preserve_index=False)
        else:
            table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(out_path), compression="snappy")
        logger.info("Parquet guardado: %s (%d filas)", out_path, len(df))
        return out_path
    except Exception as exc:
        logger.error("Error al guardar Parquet para %s: %s", table_type, exc)
        return None


def cleanup_old_raw(retention_hours: int = RETENTION_HOURS) -> None:
    """
    Elimina archivos Parquet RAW más antiguos que `retention_hours` horas.
    Recorre el directorio PARQUET_DIR y borra archivos cuyo mtime supere el umbral.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)
    deleted = 0
    for path in PARQUET_DIR.rglob("*.parquet"):
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                path.unlink()
                deleted += 1
        except Exception as exc:
            logger.warning("No se pudo eliminar %s: %s", path, exc)
    if deleted:
        logger.info("Limpieza: %d archivos Parquet eliminados (>%dh de antigüedad)", deleted, retention_hours)


# ─── Tarea principal ──────────────────────────────────────────────────────────

def run_pipeline() -> None:
    """
    Ejecuta un ciclo completo del loader:
    1. Obtiene la lista de archivos del lastupdate.
    2. Descarga y parsea cada uno.
    3. Guarda en Parquet.
    4. Limpia datos RAW antiguos.
    """
    now = datetime.now(tz=timezone.utc)
    logger.info("═══ Iniciando ciclo del loader — %s ═══", now.isoformat())

    files = fetch_lastupdate()
    if not files:
        logger.warning("No se encontraron archivos para descargar en este ciclo.")
        return

    for file_info in files:
        df = download_and_parse(file_info)
        if df is None or df.empty:
            continue
        df = cast_numeric_columns(df, file_info["type"])
        save_parquet(df, file_info["type"], now)

    cleanup_old_raw()
    logger.info("═══ Ciclo completado ═══\n")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Si se invoca con el argumento --once, ejecuta un solo ciclo y termina.
    # Útil para Airflow u orquestación externa.
    if "--once" in sys.argv:
        run_pipeline()
        sys.exit(0)

    # Modo loop: corre cada INTERVAL_MINUTES minutos
    logger.info(
        "Loader iniciado. Correrá cada %d minutos. Parquet en: %s",
        INTERVAL_MINUTES, PARQUET_DIR,
    )
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Primer ciclo inmediato al arrancar
    run_pipeline()

    schedule.every(INTERVAL_MINUTES).minutes.do(run_pipeline)
    while True:
        schedule.run_pending()
        time.sleep(30)
