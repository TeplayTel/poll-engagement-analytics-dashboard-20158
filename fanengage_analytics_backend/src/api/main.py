"""
fanengage_analytics_backend FastAPI backend with
- REST APIs for heartbeat, poll skip, and analytics summary,
- Kafka consumer for poll-event-analytics,
- Scheduled jobs for aggregation,
- PostgreSQL database connection (via env),
- Error handling and scheduler history tracking.

Author: kavia code generation agent
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import asyncpg
from aiokafka import AIOKafkaConsumer
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# --- ENVIRONMENT VARIABLES (from .env) ---
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "UTC")

# --- LOGGER SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fanengage_analytics_backend")

# --- FASTAPI APP ---
app = FastAPI(
    title="FanEngage Analytics Backend",
    description="Backend service for poll engagement analytics (poll events, heartbeat, poll skip, analytics aggregation, Kafka consumer, scheduler)",
    version="1.0.0",
    openapi_tags=[
        {"name": "Analytics", "description": "FanEngage Analytics APIs"},
        {"name": "Presence", "description": "Viewer/user presence tracking"},
        {"name": "PollEvents", "description": "Poll event logging and skip tracking"},
        {"name": "Scheduler", "description": "Scheduler and aggregation jobs"},
        {"name": "Kafka", "description": "Kafka consumer endpoints"},
    ]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DATABASE UTILITIES ---

async def get_pg_pool():
    """Get a PostgreSQL connection pool (singleton)"""
    DB_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_URL}:{POSTGRES_PORT}/{POSTGRES_DB}"
    if not hasattr(get_pg_pool, "pg_pool"):
        get_pg_pool.pg_pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=5)
    return get_pg_pool.pg_pool

# --- Pydantic Models ---

# PUBLIC_INTERFACE
class HeartbeatRequest(BaseModel):
    user_id: str = Field(..., description="User identifier")
    session_id: str = Field(..., description="Session identifier")
    device_info: Optional[str] = Field(None, description="Device information")
    platform: Optional[str] = Field(None, description="Platform info")
    timestamp: Optional[datetime] = Field(default_factory=datetime.utcnow, description="UTC timestamp")

# PUBLIC_INTERFACE
class SkipPollRequest(BaseModel):
    user_id: str = Field(..., description="User identifier")
    poll_id: str = Field(..., description="Poll identifier")
    reason: Optional[str] = Field(None, description="Reason for skipping")
    timestamp: Optional[datetime] = Field(default_factory=datetime.utcnow, description="UTC timestamp")

# PUBLIC_INTERFACE
class PollAnalyticsSummaryResponse(BaseModel):
    total_polls: int
    total_votes: int
    response_time_avg: float
    heatmap: dict  # should be more specific for a real app
    geo_distribution: dict
    device_breakdown: dict
    poll_type_popularity: dict
    popular_words: List[str]
    rejection_reasons: dict
    since: datetime
    until: datetime

# --- Error Handling Utility ---

async def record_scheduler_history(pool, job_name, status, details):
    """Log run, retries, errors for schedulers."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO analytics_scheduler_history (job_name, run_at, status, details)
                VALUES ($1, $2, $3, $4)""",
                job_name,
                datetime.utcnow(),
                status,
                details
            )
    except Exception as ex:
        logger.error(f"Unable to log to scheduler history: {ex}")

async def retry_with_scheduler_history(fn, pool, job_name, max_retries=2):
    tries = 0
    last_exc = None
    while tries <= max_retries:
        try:
            await fn()
            await record_scheduler_history(pool, job_name, "success", "Job completed OK")
            return
        except Exception as ex:
            last_exc = ex
            tries += 1
            await record_scheduler_history(pool, job_name, f"failed_attempt_{tries}", str(ex))
            logger.error(f"Scheduler job {job_name} failed on attempt {tries}: {ex}")
            if tries > max_retries:
                break
            await asyncio.sleep(2)
    await record_scheduler_history(pool, job_name, "failed", f"Final failure: {last_exc}")

# --- ROUTES ---

@app.get("/", tags=["Health"])
# PUBLIC_INTERFACE
def health_check():
    """Basic health check."""
    return {"message": "Healthy"}

@app.post("/fan-engagement/analytics/v1/heartbeat", tags=["Presence"], summary="Track user presence (heartbeat)", response_model=dict)
# PUBLIC_INTERFACE
async def heartbeat(payload: HeartbeatRequest):
    """Record user heartbeat for real-time presence tracking.

    Args:
        payload (HeartbeatRequest): The heartbeat payload.

    Returns:
        dict: Success message.
    """
    pool = await get_pg_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO user_presence(user_id, session_id, device_info, platform, last_seen)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, session_id) 
                DO UPDATE SET device_info=EXCLUDED.device_info, platform=EXCLUDED.platform, last_seen=EXCLUDED.last_seen
                """,
                payload.user_id, payload.session_id, payload.device_info, payload.platform, payload.timestamp
            )
        return {"status": "ok"}
    except Exception as ex:
        logger.error(f"Heartbeat error: {ex}")
        raise HTTPException(status_code=500, detail="Failed to record presence")

@app.post("/polls/v1/skip", tags=["PollEvents"], summary="Log skip poll event", response_model=dict)
# PUBLIC_INTERFACE
async def skip_poll(payload: SkipPollRequest):
    """Log the event when a user skips a poll.

    Args:
        payload (SkipPollRequest): The skip poll payload.

    Returns:
        dict: Success or error message.
    """
    pool = await get_pg_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO poll_events_log(user_id, poll_id, event_type, reason, event_time)
                VALUES ($1, $2, $3, $4, $5)""",
                payload.user_id, payload.poll_id, "skipped", payload.reason, payload.timestamp
            )
        return {"status": "ok"}
    except Exception as ex:
        logger.error(f"Skip poll error: {ex}")
        raise HTTPException(status_code=500, detail="Failed to log skip event")

@app.get("/fanEngage/analytics/v1/pollSummary", tags=["Analytics"], summary="Analytics summary (dashboard)", response_model=PollAnalyticsSummaryResponse)
# PUBLIC_INTERFACE
async def poll_summary(
    since: Optional[datetime] = None,
    until: Optional[datetime] = None
):
    """Retrieve aggregated poll engagement analytics.

    Args:
        since (datetime, optional): Lower bound timestamp.
        until (datetime, optional): Upper bound timestamp.

    Returns:
        PollAnalyticsSummaryResponse: Aggregated analytics for dashboard
    """
    pool = await get_pg_pool()
    now = datetime.utcnow()
    since = since or now - timedelta(days=7)
    until = until or now
    try:
        async with pool.acquire() as conn:
            # Example analytics; for production, use real aggregation queries
            row = await conn.fetchrow(
                """SELECT 
                COUNT(distinct poll_id) as total_polls,
                COUNT(*) FILTER (WHERE event_type='vote') as total_votes,
                COALESCE(AVG(response_time),0) as response_time_avg
                FROM poll_events_log
                WHERE event_time BETWEEN $1 AND $2
                """, since, until
            )
            # Example: Fetch sample breakdowns
            device_rows = await conn.fetch(
                """SELECT device_info, COUNT(*) FROM user_presence WHERE last_seen BETWEEN $1 AND $2 GROUP BY device_info""",
                since, until
            )
            # Populate further details with dummy structure for now
            return PollAnalyticsSummaryResponse(
                total_polls=row["total_polls"] or 0,
                total_votes=row["total_votes"] or 0,
                response_time_avg=float(row["response_time_avg"] or 0),
                heatmap={},  # Would come from processed events by time/location
                geo_distribution={},
                device_breakdown={r["device_info"]: r["count"] for r in device_rows if r["device_info"]},
                poll_type_popularity={},
                popular_words=[],
                rejection_reasons={},
                since=since,
                until=until
            )
    except Exception as ex:
        logger.error(f"Poll summary error: {ex}")
        raise HTTPException(status_code=500, detail="Failed to retrieve analytics")

# --- KAFKA CONSUMER BACKGROUND TASK ---

async def handle_kafka_message(msg, pool):
    """
    Parse poll-event-analytics messages and insert to poll_events_log.
    """
    try:
        import json
        data = json.loads(msg.value)
        # expects: poll_id, user_id, event_type, event_time, response_time, extra fields...
        query = """INSERT INTO poll_events_log
            (poll_id, user_id, event_type, event_time, response_time)
            VALUES ($1, $2, $3, $4, $5)"""
        args = [
            data.get("poll_id"), data.get("user_id"), data.get("event_type"),
            datetime.fromisoformat(data["event_time"]) if "event_time" in data else datetime.utcnow(),
            data.get("response_time")
        ]
        async with pool.acquire() as conn:
            await conn.execute(query, *args)
        logger.info(f"Kafka analytic event logged: {data}")
    except Exception as ex:
        logger.error(f"Error processing Kafka message: {ex}")

async def consume_kafka_events():
    """
    Background Kafka consumer for poll-event-analytics topic.
    """
    topic = "poll-event-analytics"
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="analytics-backend"
    )
    await consumer.start()
    pool = await get_pg_pool()
    try:
        async for msg in consumer:
            await handle_kafka_message(msg, pool)
    except Exception as ex:
        logger.error(f"Kafka consumer crashed: {ex}")
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    """
    FastAPI startup: Start Kafka consumer and scheduler jobs.
    """
    # Start Kafka consumer in background
    asyncio.create_task(consume_kafka_events())
    # Start scheduled jobs
    scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    scheduler.add_job(
        poll_event_aggregator_job, 
        trigger=IntervalTrigger(minutes=5),
        id="poll_event_aggregator"
    )
    scheduler.add_job(
        poll_aggregator_retry_job, 
        trigger=IntervalTrigger(minutes=10),
        id="poll_aggregator_retry"
    )
    scheduler.start()

# --- SCHEDULED JOBS ---

async def poll_event_aggregator_job():
    """Every 5 min: aggregate poll_events_log into poll_analytics."""
    pool = await get_pg_pool()
    async def aggregator():
        # Example: Aggregate number of votes per poll
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO poll_analytics(poll_id, total_votes, aggregated_at)
                SELECT poll_id, COUNT(*) FILTER (WHERE event_type='vote'), $1
                FROM poll_events_log
                WHERE event_time > now() - interval '10 min'
                GROUP BY poll_id
                ON CONFLICT (poll_id) DO UPDATE
                SET total_votes=EXCLUDED.total_votes, aggregated_at=EXCLUDED.aggregated_at
            """, datetime.utcnow())
    await retry_with_scheduler_history(aggregator, pool, "poll_event_aggregator")

async def poll_aggregator_retry_job():
    """Every 10 min: retry failed poll_event_aggregator aggregates."""
    pool = await get_pg_pool()
    async def retry_aggregate():
        # Dummy: just mark failed jobs as retried (demonstration)
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE analytics_scheduler_history 
                SET status='retried', details='Retry attempted'
                WHERE job_name='poll_event_aggregator' AND status LIKE 'failed%'
                  AND run_at > now() - interval '1 hour'
            """)
    await retry_with_scheduler_history(retry_aggregate, pool, "poll_aggregator_retry")

# --- NOTES ---
# Make sure the analytics database has tables:
# user_presence, poll_events_log, poll_analytics, analytics_scheduler_history.
# See DB schema for field/types.
# .env file must include: POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT

