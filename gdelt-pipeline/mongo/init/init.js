// ─────────────────────────────────────────────────────────────────────────────
// mongo/init/init.js — Inicialización de MongoDB para GDELT Pipeline
// Se ejecuta automáticamente al primer arranque del contenedor.
//
// Define todas las colecciones con sus schemas (validación), índices y TTL.
// Los datos son pre-calculados por Spark — el dashboard solo lee.
// ─────────────────────────────────────────────────────────────────────────────

// Cambiar a la base de datos del proyecto
db = db.getSiblingDB(process.env.MONGO_INITDB_DATABASE || "gdelt");

print("═══ Inicializando base de datos GDELT ═══");

// ─── Helper para crear colección con validación opcional ──────────────────────
function createCollection(name, validator, description) {
  if (!db.getCollectionNames().includes(name)) {
    if (validator) {
      db.createCollection(name, { validator: validator });
    } else {
      db.createCollection(name);
    }
    print("✔ Colección creada: " + name + " — " + description);
  } else {
    print("  Colección ya existe: " + name);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 1 — Mapa de calor de conflictos por país/día
// Campos: country (ISO2), date (YYYYMMDD int), avg_goldstein, event_count
// ─────────────────────────────────────────────────────────────────────────────
createCollection("conflict_heatmap", null, "Intensidad de conflictos por país/día (Goldstein)");
db.conflict_heatmap.createIndex({ country: 1, date: -1 });
db.conflict_heatmap.createIndex({ date: -1 });
db.conflict_heatmap.createIndex({ avg_goldstein: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 2 — Top 10 países por eventos noticiosos por día
// Campos: country, date, event_count, rank
// ─────────────────────────────────────────────────────────────────────────────
createCollection("top_news_countries", null, "Top 10 países generadores de noticias por día");
db.top_news_countries.createIndex({ date: -1, rank: 1 });
db.top_news_countries.createIndex({ country: 1, date: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 3 — Correlación AvgTone vs. NumSources
// Campos: country, date, avg_tone, avg_sources, tone_sources_correlation, sample_size
// ─────────────────────────────────────────────────────────────────────────────
createCollection("tone_source_correlation", null, "Correlación tono promedio vs fuentes noticiosas");
db.tone_source_correlation.createIndex({ country: 1, date: -1 });
db.tone_source_correlation.createIndex({ tone_sources_correlation: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 4 — Distribución CAMEO por región
// Campos: EventRootCode, event_description, region, date, event_count
// ─────────────────────────────────────────────────────────────────────────────
createCollection("cameo_distribution", null, "Distribución de tipos CAMEO por región del mundo");
db.cameo_distribution.createIndex({ region: 1, date: -1 });
db.cameo_distribution.createIndex({ EventRootCode: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 5 — Matriz de interacción entre tipos de actores
// Campos: actor1_type, actor2_type, event_count, avg_goldstein, avg_tone
// ─────────────────────────────────────────────────────────────────────────────
createCollection("actor_interaction_matrix", null, "Matriz GOV×MIL×REB — frecuencia y tono");
db.actor_interaction_matrix.createIndex({ actor1_type: 1, actor2_type: 1 });
db.actor_interaction_matrix.createIndex({ event_count: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 6 — Cobertura mediática por evento (menciones/evento)
// Campos: country, date, mentions_per_event, total_mentions, total_events
// ─────────────────────────────────────────────────────────────────────────────
createCollection("media_coverage_ratio", null, "Razón menciones/evento por país");
db.media_coverage_ratio.createIndex({ country: 1, date: -1 });
db.media_coverage_ratio.createIndex({ mentions_per_event: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 7 — Tendencia de sentimiento (promedio móvil AvgTone)
// Campos: country, date, avg_tone, moving_avg_3d
// ─────────────────────────────────────────────────────────────────────────────
createCollection("sentiment_trend", null, "Tendencia del tono mediático con promedio móvil 7d");
db.sentiment_trend.createIndex({ country: 1, date: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 8 — Pares de países en conflicto
// Campos: actor1_country, actor2_country, date, conflict_count, avg_goldstein
// ─────────────────────────────────────────────────────────────────────────────
createCollection("conflict_pairs", null, "Pares de países en conflicto frecuente");
db.conflict_pairs.createIndex({ actor1_country: 1, actor2_country: 1 });
db.conflict_pairs.createIndex({ conflict_count: -1 });
db.conflict_pairs.createIndex({ date: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 9 — Detección de escalada de eventos
// Campos: GlobalEventID, mention_hour, mentions_last_24h, mentions_this_hour, escalation_ratio
// ─────────────────────────────────────────────────────────────────────────────
createCollection("escalation_events", null, "Eventos con aumento acelerado de menciones en 24h");
db.escalation_events.createIndex({ escalation_ratio: -1 });
db.escalation_events.createIndex({ mention_hour: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 10 — Conflictos por religión y región
// Campos: religion, region, date, event_count, avg_goldstein
// ─────────────────────────────────────────────────────────────────────────────
createCollection("religion_conflict_cluster", null, "Eventos de conflicto agrupados por religión/región");
db.religion_conflict_cluster.createIndex({ religion: 1, region: 1 });
db.religion_conflict_cluster.createIndex({ event_count: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 11 — Temas GKG por continente por año
// Campos: theme, continent, year, mention_count, rank
// ─────────────────────────────────────────────────────────────────────────────
createCollection("gkg_themes_continent", null, "Top temas del GKG por continente y año");
db.gkg_themes_continent.createIndex({ continent: 1, year: -1, rank: 1 });
db.gkg_themes_continent.createIndex({ theme: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 12 — Organizaciones más mencionadas por día
// Campos: organization, date, mention_count, rank
// ─────────────────────────────────────────────────────────────────────────────
createCollection("top_organizations", null, "Top organizaciones mencionadas a nivel global");
db.top_organizations.createIndex({ date: -1, rank: 1 });
db.top_organizations.createIndex({ organization: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 13 — Rezago tono → conflicto
// Campos: country, date, today_avg_tone, today_conflict_count, tomorrow_conflict_count
// ─────────────────────────────────────────────────────────────────────────────
createCollection("tone_conflict_lag", null, "Análisis de rezago: tono de hoy predice conflicto mañana");
db.tone_conflict_lag.createIndex({ country: 1, date: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 14 — Red diplomática vs. conflicto entre países
// Campos: source_country, target_country, diplomatic_count, conflict_count, total_interactions
// ─────────────────────────────────────────────────────────────────────────────
createCollection("diplomatic_network", null, "Red de interacciones diplomáticas vs conflictos");
db.diplomatic_network.createIndex({ source_country: 1, target_country: 1 });
db.diplomatic_network.createIndex({ conflict_count: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 15 — Índice de diversidad de fuentes por país
// Campos: country, date, unique_sources, total_mentions, source_diversity_index
// ─────────────────────────────────────────────────────────────────────────────
createCollection("source_diversity_index", null, "Índice de diversidad de medios por país");
db.source_diversity_index.createIndex({ country: 1, date: -1 });
db.source_diversity_index.createIndex({ source_diversity_index: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 16 — Conflictos por etnia
// Campos: ethnicity, date, event_count, avg_goldstein
// ─────────────────────────────────────────────────────────────────────────────
createCollection("ethnic_conflicts", null, "Frecuencia de conflictos por grupo étnico");
db.ethnic_conflicts.createIndex({ ethnicity: 1, date: -1 });
db.ethnic_conflicts.createIndex({ event_count: -1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 17 — Noticias de última hora
// Campos: GlobalEventID, first_mention_time, mentions_in_first_hour, country, date
// ─────────────────────────────────────────────────────────────────────────────
createCollection("breaking_news", null, "Eventos que pasan de 0 a +100 menciones en <1h");
db.breaking_news.createIndex({ first_mention_time: -1 });
db.breaking_news.createIndex({ mentions_in_first_hour: -1 });
db.breaking_news.createIndex({ country: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 18 [Extra] — Top eventos con tono más extremo del periodo
// Campos: GlobalEventID, country, EventRootCode, QuadClass, AvgTone,
//         NumMentions, Day, tone_extreme_type ("most_positive"/"most_negative")
// ─────────────────────────────────────────────────────────────────────────────
createCollection("extreme_tone_events", null, "[Extra] Eventos con el tono más extremo (positivo/negativo) del periodo");
db.extreme_tone_events.createIndex({ tone_extreme_type: 1, AvgTone: 1 });
db.extreme_tone_events.createIndex({ country: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS 19 [Extra] — Ranking de países por cobertura de artículos
// Campos: country, total_articles, avg_articles_per_event, event_count
// ─────────────────────────────────────────────────────────────────────────────
createCollection("top_article_coverage", null, "[Extra] Países con mayor cobertura total de artículos");
db.top_article_coverage.createIndex({ total_articles: -1 });
db.top_article_coverage.createIndex({ country: 1 });

// ─────────────────────────────────────────────────────────────────────────────
// METADATOS — registro de ejecuciones del pipeline
// ─────────────────────────────────────────────────────────────────────────────
createCollection("pipeline_runs", null, "Historial de ejecuciones del pipeline (loader + Spark)");
db.pipeline_runs.createIndex({ started_at: -1 });
db.pipeline_runs.createIndex({ status: 1 });

print("═══ Inicialización completada. Colecciones: " + db.getCollectionNames().length + " ═══");
