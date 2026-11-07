# Real-Time Sensor Pipeline with Kafka, PostgreSQL, FastAPI and React

An end-to-end streaming data project. Simulated IoT temperature sensors publish events to Apache Kafka, several independent consumers process the same stream (anomaly detection, statistics, and storage), and the stored data is served through a REST API to a React dashboard.

Built as a hands-on learning project to understand Kafka's publish/subscribe model, partitions and consumer groups, then extended with persistence, an API and a dashboard.

## Architecture

```
producer.py ──► [ sensor-readings topic · 3 partitions ]
                   │                │                     │
     anomaly-detector-group  stats-aggregator-group  postgres-storage-group
                   │                │                     │
        anomaly_detector.py  stats_aggregator.py   storage_consumer.py
                   │          (console summary)            │
                   ▼                                       ▼
        [ anomaly-alerts topic ]                      PostgreSQL
          (dead letter topic)                              │
                                                    api.py (FastAPI)
                                                           │
                                                  dashboard/ (React + Vite)
```

Each consumer group receives **every** event independently, so the services never interfere with each other.

## Project structure

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Kafka 4.3.1 broker (KRaft mode, no ZooKeeper) and PostgreSQL 17 |
| `producer.py` | Streams one JSON event per second from 5 sensors, keyed by `sensor_id`. About 10% of readings are injected spikes (150-200 °C) labelled with `is_anomaly` |
| `consumer_basic.py` | Demo consumer used to observe partition rebalancing when two consumers share a group |
| `anomaly_detector.py` | Per-sensor z-score detection (threshold 2.0, 5-reading warmup). Forwards flagged events to the `anomaly-alerts` topic |
| `stats_aggregator.py` | Running average, min, max and count per sensor, printed every 10 events |
| `storage_consumer.py` | Persists every event to PostgreSQL, committing the Kafka offset only after a successful database write |
| `api.py` | FastAPI service exposing readings and aggregated statistics |
| `dashboard/` | React 19 + Vite dashboard |
| `schema.sql` | Database table definition |

## Tech stack

Python · Apache Kafka · confluent-kafka · PostgreSQL · psycopg2 · FastAPI · React · Vite · Docker Compose

## Getting started

### Prerequisites

- Python 3.9+
- Node.js 18+
- Docker Desktop (running)

### 1. Configure environment variables

```powershell
copy .env.example .env
```

Open `.env` and set `DB_PASSWORD` to a password of your choice. `.env` is git-ignored.

### 2. Start Kafka and PostgreSQL

```powershell
docker compose up -d
```

### 3. Create the database table

```powershell
Get-Content schema.sql | docker exec -i sensor-db psql -U sensor_user -d sensor_pipeline
```

### 4. Install Python dependencies

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 5. Run the pipeline

Open a terminal for each (virtual environment activated, project root):

```powershell
python producer.py            # stream sensor data
python anomaly_detector.py    # detect anomalies, publish to anomaly-alerts
python stats_aggregator.py    # running statistics in the console
python storage_consumer.py    # persist events to PostgreSQL
uvicorn api:app --reload      # REST API on http://127.0.0.1:8000
```

### 6. Run the dashboard

```powershell
cd dashboard
npm install
npm run dev
```

Open http://localhost:5173. The interactive API docs are at http://127.0.0.1:8000/docs.

### Inspect the dead letter topic

```powershell
docker exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomaly-alerts --from-beginning
```

### Shut down

```powershell
docker compose down
```

Add `-v` to also delete the stored database volume.

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Service info |
| `GET /health` | Checks the database connection |
| `GET /readings?limit=100&sensor_id=sensor-1` | Latest readings, optionally filtered by sensor |
| `GET /stats` | Totals, anomaly count, average, min and max temperature |
| `GET /stats/sensors` | Per-sensor aggregates |
| `GET /stats/timeline?limit=60` | Per-minute average temperature and anomaly counts |

## Known limitations and ideas for improvement

- **Dashboard anomaly counts are ground-truth labels.** The producer attaches `is_anomaly` and that is what gets stored. The z-score detector's verdicts are not persisted, so detector accuracy (precision and recall) is not measured yet.
- **The dashboard loads data once.** It does not auto-refresh; reload the page to see new data.
- **At-least-once storage.** The storage consumer commits the offset after the database write, so a crash between the two could insert a duplicate row. A unique constraint or idempotent insert would fix this.
- **Detector baseline contamination.** Every reading, including spikes, is added to a sensor's history, so repeated spikes inflate the mean and standard deviation. Excluding flagged readings, and using a rolling window instead of unbounded history, would help.
- **Not production hardened.** No schema registry, authentication on the API, retries or monitoring.