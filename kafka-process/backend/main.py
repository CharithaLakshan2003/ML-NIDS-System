"""
Hybrid NIDS - Backend API
==========================
The query/serving layer on top of the storage backends this pipeline writes
to. It does NOT sit in the hot path (Kafka -> Flink -> OpenSearch/MinIO
still happens exactly as before, via alert_indexer.py / datalake_writer.py)
- this is what a frontend/SOC dashboard talks to.

Endpoints
---------
GET  /health                     liveness of the API + its two backends
GET  /alerts                     search/filter indexed alerts (OpenSearch)
GET  /alerts/{doc_id}             fetch a single alert by its indexed id
GET  /stats/severity              alert counts by severity_tier over a window
GET  /datalake/objects            list Parquet files in the MinIO data lake
WS   /ws/alerts                   live alert feed, pushed the moment
                                   fusion_job.py emits to nids-alerts

Run: uvicorn backend.main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from opensearchpy import OpenSearch

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../services")
from storage_common import (  # noqa: E402
    ALERTS_INDEX_PREFIX,
    KAFKA_BOOTSTRAP,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    OPENSEARCH_HOST,
    OPENSEARCH_PORT,
    OPENSEARCH_USE_SSL,
    TOPIC_NIDS_ALERTS,
)

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("nids_backend")

app = FastAPI(title="Hybrid NIDS Backend", version="1.0")

os_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_compress=True,
    use_ssl=OPENSEARCH_USE_SSL,
    verify_certs=False,
    ssl_show_warn=False,
)

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    opensearch_ok = True
    minio_ok = True
    try:
        os_client.cluster.health()
    except Exception:  # noqa: BLE001 - health check, report status don't crash
        opensearch_ok = False
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:  # noqa: BLE001
        minio_ok = False
    return {"opensearch": opensearch_ok, "minio": minio_ok}


# ---------------------------------------------------------------------------
# GET /alerts
# ---------------------------------------------------------------------------
@app.get("/alerts")
def search_alerts(
    severity_tier: Optional[str] = Query(None, description="CRITICAL, HIGH, MEDIUM, or LOW"),
    src_ip: Optional[str] = None,
    dst_ip: Optional[str] = None,
    since_minutes: int = Query(60, ge=1, le=60 * 24 * 30, description="How far back to search"),
    limit: int = Query(50, ge=1, le=1000),
):
    """Search recent alerts, most recent first. All filters are optional and AND-combined."""
    must = [{"range": {"@timestamp": {"gte": "now-{}m".format(since_minutes)}}}]
    if severity_tier:
        must.append({"term": {"severity_tier": severity_tier.upper()}})
    if src_ip:
        must.append({"term": {"src_ip": src_ip}})
    if dst_ip:
        must.append({"term": {"dst_ip": dst_ip}})

    query = {
        "query": {"bool": {"must": must}},
        "sort": [{"@timestamp": {"order": "desc"}}],
        "size": limit,
    }

    try:
        result = os_client.search(index="{}-*".format(ALERTS_INDEX_PREFIX), body=query)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="OpenSearch query failed: {}".format(e))

    hits = result.get("hits", {}).get("hits", [])
    return {
        "total": result.get("hits", {}).get("total", {}).get("value", 0),
        "alerts": [{"id": h["_id"], **h["_source"]} for h in hits],
    }


# ---------------------------------------------------------------------------
# GET /alerts/{doc_id}
# ---------------------------------------------------------------------------
@app.get("/alerts/{doc_id}")
def get_alert(doc_id: str):
    try:
        result = os_client.search(
            index="{}-*".format(ALERTS_INDEX_PREFIX),
            body={"query": {"ids": {"values": [doc_id]}}, "size": 1},
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="OpenSearch query failed: {}".format(e))

    hits = result.get("hits", {}).get("hits", [])
    if not hits:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"id": hits[0]["_id"], **hits[0]["_source"]}


# ---------------------------------------------------------------------------
# GET /stats/severity
# ---------------------------------------------------------------------------
@app.get("/stats/severity")
def severity_stats(since_minutes: int = Query(60, ge=1, le=60 * 24 * 30)):
    """Alert counts grouped by severity tier - what a dashboard summary tile needs."""
    query = {
        "query": {"range": {"@timestamp": {"gte": "now-{}m".format(since_minutes)}}},
        "size": 0,
        "aggs": {"by_severity": {"terms": {"field": "severity_tier"}}},
    }
    try:
        result = os_client.search(index="{}-*".format(ALERTS_INDEX_PREFIX), body=query)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="OpenSearch query failed: {}".format(e))

    buckets = result.get("aggregations", {}).get("by_severity", {}).get("buckets", [])
    return {b["key"]: b["doc_count"] for b in buckets}


# ---------------------------------------------------------------------------
# GET /datalake/objects
# ---------------------------------------------------------------------------
@app.get("/datalake/objects")
def list_datalake_objects(
    date: Optional[str] = Query(None, description="YYYY-MM-DD, defaults to today (UTC)"),
    limit: int = Query(100, ge=1, le=1000),
):
    """List Parquet files the data-lake writer has produced for a given day."""
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = "engineered-features/dt={}/".format(date)

    try:
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix, MaxKeys=limit)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="MinIO list failed: {}".format(e))

    objects = [
        {"key": obj["Key"], "size_bytes": obj["Size"], "last_modified": obj["LastModified"].isoformat()}
        for obj in response.get("Contents", [])
    ]
    return {"bucket": MINIO_BUCKET, "prefix": prefix, "count": len(objects), "objects": objects}


# ---------------------------------------------------------------------------
# WS /ws/alerts - live feed straight from Kafka, independent of OpenSearch
# indexing latency
# ---------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:  # noqa: BLE001 - a broken client shouldn't kill the broadcast
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # This endpoint is push-only; we still need to await something
            # so a client disconnect (WebSocketDisconnect) is detected.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


async def kafka_to_websocket_bridge():
    """Background task: tails nids-alerts and pushes every message to connected clients."""
    consumer = AIOKafkaConsumer(
        TOPIC_NIDS_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="backend-ws-bridge",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )
    await consumer.start()
    logger.info("WebSocket bridge connected to Kafka topic '%s'.", TOPIC_NIDS_ALERTS)
    try:
        async for message in consumer:
            await manager.broadcast(message.value)
    finally:
        await consumer.stop()


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(kafka_to_websocket_bridge())
