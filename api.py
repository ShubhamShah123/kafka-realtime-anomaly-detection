from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(
    title="Real-Time Sensor Pipeline API",
    description="API for retreiving sensor readings from PostgreSQL",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "sensor_pipeline"),
        user=os.getenv("DB_USER", "sensor_user"),
        password=os.environ["DB_PASSWORD"],
    )

# URLS
@app.get("/")
def home():
    return {
        "message": "Sensor Pipeline API is running.",
        "docs": "/docs"
    }

@app.get("/health")
def health():
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {str(e)}"
        )

@app.get("/readings")
def get_readings(
    limit: int = Query(default=100, ge=1, le=1000),
    sensor_id: Optional[str] = None,
):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if sensor_id:
                cursor.execute(
                    """
                    SELECT id, sensor_id, temperature, event_time, is_anomaly, created_at
                    FROM sensor_readings
                    WHERE sensor_id = %s
                    ORDER BY event_time DESC, id DESC
                    LIMIT %s
                    """,
                    (sensor_id, limit)
                ) 
            else:
                cursor.execute(
                    """
                    SELECT id, sensor_id, temperature, event_time, is_anomaly, created_at
                    FROM sensor_readings
                    ORDER BY event_time DESC, id DESC
                    LIMIT %s
                    """,
                    (limit,)
                )
            readings = cursor.fetchall()
        return {
            "count": len(readings),
            "readings": readings,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not retrieve readings: {str(e)}"
        )
    finally:
        if conn is not None:
            conn.close()

# Stats
@app.get('/stats')
def get_stats():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT
                COUNT(*) AS total_readings,
                COUnt(*) FILTER (
                    WHERE is_anomaly = TRUE
                ) AS anomaly_count,
                COUNT(*) FILTER (
                    WHERE is_anomaly = FALSE
                ) AS normal_count,
                ROUND(AVG(temperature)::numeric, 2)
                    AS average_temperature,
                MIN(temperature) AS minimum_temp,
                MAX(temperature) AS maximum_temp
            FROM sensor_readings;
            """)
            stats = cursor.fetchone()
        total = stats["total_readings"]
        anomalies = stats["anomaly_count"]
        stats["anomaly_percentage"] = (
            round(anomalies / total * 100, 2)
            if total else 0
        )
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not retreive stats: {str(e)}"
        )
    finally:
        if conn is not None:
            conn.close()

@app.get('/stats/sensors')
def get_stats_sensors():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT 
                    sensor_id,
                    COUNT(*) AS total_readings,
                    ROUND(AVG(temperature)::numeric, 2) AS average_temperature,
                    MIN(temperature) AS minimum_temperature,
                    MAX(temperature) AS maximum_temperature,
                    COUNT(*) FILTER ( WHERE is_anomaly = TRUE ) AS anomaly_count
                FROM sensor_readings
                GROUP BY sensor_id
                ORDER BY sensor_id;
                """
            )
            sensors = cursor.fetchall()
        return {
            "sensor_count": len(sensors),
            "sensors": sensors
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not retreive stats: {str(e)}"
        )
    finally:
        if conn is not None:
            conn.close()

@app.get('/stats/timeline')
def get_timeline(limit: int = Query(default=60, ge=1, le=1440)):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                WITH minute_stats AS (
                    SELECT
                        date_trunc('minute', event_time) AS time_bucket,
                        COUNT(*) AS reading_count,
                        ROUND(AVG(temperature)::numeric, 2) AS average_temperature,
                        COUNT(*) FILTER (WHERE is_anomaly = TRUE) AS anomaly_count
                    FROM sensor_readings
                    GROUP BY date_trunc('minute', event_time)
                )
                SELECT
                    time_bucket,
                    reading_count,
                    average_temperature,
                    anomaly_count
                FROM minute_stats
                ORDER BY time_bucket DESC
                LIMIT %s

                """,(limit,)
            )
            timeline = cursor.fetchall()
        timeline.reverse()
        return {
            "interval": "1 minute",
            "count": len(timeline),
            "timeline": timeline
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not retrieve timeline: {str(e)}"
        )
    finally:
        if conn is not None: 
            conn.close()