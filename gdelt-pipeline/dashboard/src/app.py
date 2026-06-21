"""
app.py — Dashboard Flask para GDELT Pipeline
=============================================
Conecta a MongoDB y expone rutas para cada grupo de análisis.
El dashboard NO calcula nada — solo lee y visualiza resultados pre-calculados.

Rutas:
  /                      → Resumen general / landing
  /conflictos            → Análisis 1, 8, 9, 14, 16
  /cobertura             → Análisis 2, 6, 12, 15, 17
  /actores               → Análisis 4, 5, 10
  /sentimiento           → Análisis 3, 7, 11, 13
  /tendencias            → Análisis 18, 19 (extra)
  /conclusiones          → Conclusiones del grupo
  /api/<collection>      → Endpoint JSON genérico
"""

import json
import os
from functools import lru_cache

from bson import json_util
from flask import Flask, jsonify, render_template
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/gdelt")
MONGO_DB  = os.getenv("MONGO_DB", "gdelt")

# ─── Conexión a MongoDB ───────────────────────────────────────────────────────

_client: MongoClient | None = None

def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client[MONGO_DB]


def query(collection: str, filter_: dict = None, sort: list = None,
          limit: int = 500, projection: dict = None) -> list:
    """
    Ejecuta una consulta a MongoDB y retorna una lista de documentos
    con los _id convertidos a string para serialización JSON.
    """
    try:
        db = get_db()
        cursor = db[collection].find(filter_ or {}, projection or {"_id": 0})
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)
    except Exception as e:
        app.logger.error("Error en query %s: %s", collection, e)
        return []


# ─── Rutas principales ────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Landing page con resumen del pipeline."""
    try:
        db = get_db()
        stats = {
            name: db[name].estimated_document_count()
            for name in db.list_collection_names()
            if not name.startswith("system.")
        }
    except Exception:
        stats = {}
    return render_template("index.html", stats=stats)


@app.route("/conflictos")
def conflictos():
    """Sección: Conflictos (análisis 1, 8, 9, 14, 16)."""
    data = {
        "heatmap":    query("conflict_heatmap", sort=[("date", -1)], limit=200),
        "pairs":      query("conflict_pairs", sort=[("conflict_count", -1)], limit=50),
        "escalation": query("escalation_events", sort=[("escalation_ratio", -1)], limit=30),
        "network":    query("diplomatic_network", sort=[("total_interactions", -1)], limit=50),
        "ethnic":     query("ethnic_conflicts", sort=[("event_count", -1)], limit=30),
    }
    return render_template("conflictos.html", data=data)


@app.route("/cobertura")
def cobertura():
    """Sección: Cobertura Mediática (análisis 2, 6, 12, 15, 17)."""
    data = {
        "top_countries":   query("top_news_countries", sort=[("date", -1), ("rank", 1)], limit=100),
        "coverage_ratio":  query("media_coverage_ratio", sort=[("mentions_per_event", -1)], limit=50),
        "organizations":   query("top_organizations", sort=[("date", -1), ("rank", 1)], limit=100),
        "source_diversity":query("source_diversity_index", sort=[("source_diversity_index", -1)], limit=50),
        "breaking_news":   query("breaking_news", sort=[("first_mention_time", -1)], limit=30),
    }
    return render_template("cobertura.html", data=data)


@app.route("/actores")
def actores():
    """Sección: Actores (análisis 4, 5, 10)."""
    data = {
        "cameo":    query("cameo_distribution", sort=[("date", -1), ("event_count", -1)], limit=200),
        "matrix":   query("actor_interaction_matrix", sort=[("event_count", -1)], limit=100),
        "religion": query("religion_conflict_cluster", sort=[("event_count", -1)], limit=50),
    }
    return render_template("actores.html", data=data)


@app.route("/sentimiento")
def sentimiento():
    """Sección: Sentimiento y Tendencias (análisis 3, 7, 11, 13)."""
    data = {
        "correlation": query("tone_source_correlation", sort=[("date", -1)], limit=100),
        "trend": query("sentiment_trend", sort=[("country", 1), ("date", 1)], limit=2000),
        "gkg_themes":  query("gkg_themes_continent", sort=[("year", -1), ("continent", 1), ("rank", 1)], limit=200),
        "lag":         query("tone_conflict_lag", sort=[("date", -1)], limit=200),
    }
    return render_template("sentimiento.html", data=data)


@app.route("/tendencias")
def tendencias():
    """Sección: Análisis Extra (análisis 18, 19)."""
    data = {
        "extreme_tone": query("extreme_tone_events", sort=[("AvgTone", 1)], limit=30),
        "response_time": query("mention_response_time", sort=[("avg_response_minutes", 1)], limit=50),
    }
    return render_template("tendencias.html", data=data)


@app.route("/conclusiones")
def conclusiones():
    return render_template("conclusiones.html")


# ─── API JSON genérica ────────────────────────────────────────────────────────

@app.route("/api/<collection>")
def api_collection(collection: str):
    """
    Endpoint genérico que expone cualquier colección como JSON.
    Útil para depuración y para alimentar gráficos con fetch() desde el frontend.
    """
    allowed = {
        "conflict_heatmap", "top_news_countries", "tone_source_correlation",
        "cameo_distribution", "actor_interaction_matrix", "media_coverage_ratio",
        "sentiment_trend", "conflict_pairs", "escalation_events",
        "religion_conflict_cluster", "gkg_themes_continent", "top_organizations",
        "tone_conflict_lag", "diplomatic_network", "source_diversity_index",
        "ethnic_conflicts", "breaking_news", "extreme_tone_events",
        "mention_response_time",
    }
    if collection not in allowed:
        return jsonify({"error": "Colección no encontrada"}), 404

    docs = query(collection, limit=200)
    return app.response_class(
        json.dumps(docs, default=str),
        mimetype="application/json"
    )


@app.route("/health")
def health():
    try:
        get_db().command("ping")
        return jsonify({"status": "ok", "mongo": "connected"})
    except ConnectionFailure:
        return jsonify({"status": "error", "mongo": "disconnected"}), 503


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
