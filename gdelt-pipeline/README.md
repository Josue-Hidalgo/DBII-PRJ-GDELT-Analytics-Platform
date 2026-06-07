# GDELT World Event Analysis Pipeline

**IC4302 Bases de Datos II — Tarea Programada #2**  
Instituto Tecnológico de Costa Rica — II Semestre 2026

Pipeline de análisis de eventos mundiales usando datos del [GDELT Project](https://www.gdeltproject.org/), procesados con Apache Spark y visualizados en un dashboard web.

---

## Inicio Rápido

```bash
cp .env.example .env        # Configurar variables (mínimo: MONGO_PASSWORD)
docker compose up -d         # Levantar todos los servicios
open http://localhost:5000   # Ver el dashboard
```

## Servicios

| Servicio | URL | Descripción |
|---|---|---|
| Dashboard | http://localhost:5000 | Visualización de resultados |
| Spark UI | http://localhost:8080 | Monitor del cluster Spark |
| HDFS UI | http://localhost:9870 | Monitor de HDFS |
| MongoDB | localhost:27017 | Base de datos de resultados |
| Airflow* | http://localhost:8081 | Orquestación (opcional) |

\* Activar con: `docker compose --profile airflow up`

## Documentación

- [`docs/arquitectura.md`](docs/arquitectura.md) — Diagrama y flujo de datos
- [`docs/diseno_bd.md`](docs/diseno_bd.md) — Schemas de MongoDB e índices

## Estructura propuesta

```
gdelt-pipeline/
├── docker-compose.yml
├── .env.example
├── loader/          # Descarga GDELT → Parquet (Python)
├── spark/jobs/      # 19 análisis con PySpark
├── mongo/init/      # Inicialización de colecciones e índices
├── dashboard/       # Flask + Chart.js
├── airflow/dags/    # DAG opcional de orquestación
└── docs/            # Documentación técnica
```
## notas

falta implementar la parte de jobs 
falta el init de mongo, la parte del airflow que aun no sabemos

