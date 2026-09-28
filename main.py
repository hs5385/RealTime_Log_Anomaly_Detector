"""
FastAPI Server & Real-Time Log Streaming Engine.
Serves static index.html and provides a WebSocket endpoint (/ws)
streaming live logs from app.log along with anomaly detections from LogAnomalyDetector.
"""

from dotenv import load_dotenv
load_dotenv()  # Load .env before any os.getenv() calls

import asyncio
from contextlib import asynccontextmanager
import json
import logging
import os
from typing import AsyncGenerator, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from detector import LogAnomalyDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("LogStreamServer")

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "app.log")
STATIC_HTML_PATH = os.path.join(os.path.dirname(__file__), "index.html")

# Initialize LogAnomalyDetector instance
detector = LogAnomalyDetector(
    window_seconds=float(os.getenv("DETECTOR_WINDOW_SECONDS", "60.0")),
    min_baseline_samples=int(os.getenv("MIN_BASELINE_SAMPLES", "15")),
    default_baseline_mean=float(os.getenv("BASELINE_MEAN", "1.5")),
    default_baseline_std=float(os.getenv("BASELINE_STD", "1.0")),
    sns_topic_arn=os.getenv("AWS_SNS_TOPIC_ARN", None),
    cloudwatch_group=os.getenv("AWS_CLOUDWATCH_GROUP", None),
    aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    auto_establish_baseline=os.getenv("AUTO_ESTABLISH_BASELINE", "false").lower() in ("true", "1", "yes"),
)


class ConnectionManager:
    """Manages active WebSocket connections and broadcasting."""

    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info("Client connected. Total active connections: %d", len(self.active_connections))

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.active_connections.discard(websocket)
        logger.info("Client disconnected. Total active connections: %d", len(self.active_connections))

    async def broadcast(self, message: dict) -> None:
        """Broadcast JSON message to all connected clients."""
        if not self.active_connections:
            return

        payload = json.dumps(message)
        disconnected = []

        async with self._lock:
            for connection in list(self.active_connections):
                try:
                    await connection.send_text(payload)
                except Exception:
                    disconnected.append(connection)

            for dead_conn in disconnected:
                self.active_connections.discard(dead_conn)


manager = ConnectionManager()

# Bounded queue decoupling anomaly detection from AWS network I/O.
# boto3 calls are blocking; running them on the event loop would stall log tailing
# and all WebSocket broadcasts while a request is in flight.
aws_dispatch_queue: asyncio.Queue = asyncio.Queue(maxsize=250)


async def tail_log_file(file_path: str, poll_interval: float = 0.08) -> AsyncGenerator[str, None]:
    """
    Asynchronous generator that tails a continuously growing file mimicking `tail -f`.
    Accurately buffers partial lines, handles file creation, truncation, and rotation.
    """
    # Ensure log file exists
    if not os.path.exists(file_path):
        with open(file_path, "a", encoding="utf-8") as f:
            pass

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        # Follow from the end of the file
        f.seek(0, os.SEEK_END)
        last_pos = f.tell()

        while True:
            # Check for file truncation or rotation
            try:
                curr_size = os.path.getsize(file_path)
                if curr_size < last_pos:
                    logger.info("Log file truncated or rotated. Resetting position to start.")
                    f.seek(0, os.SEEK_SET)
                    last_pos = 0
            except OSError:
                pass

            line = f.readline()
            if line:
                # If line is incomplete (writer hasn't flushed newline yet), wait and re-read
                if not line.endswith("\n"):
                    f.seek(last_pos, os.SEEK_SET)
                    await asyncio.sleep(poll_interval)
                    continue

                last_pos = f.tell()
                clean_line = line.rstrip("\r\n")
                if clean_line:
                    yield clean_line
            else:
                await asyncio.sleep(poll_interval)


async def log_tailing_worker() -> None:
    """
    Background worker that continuously tails app.log, passes entries
    to LogAnomalyDetector, and broadcasts logs and alerts over WebSockets.
    """
    logger.info("Starting background log tailing on: %s", LOG_FILE_PATH)
    try:
        async for raw_line in tail_log_file(LOG_FILE_PATH):
            # Process through the anomaly detector (AWS dispatch deferred to worker)
            parsed_log, anomaly_data = detector.process_log(raw_line, aws_dispatch=False)

            # Window metrics & baseline status
            window_metrics = detector.get_window_metrics()
            baseline_status = detector.get_baseline_status()

            # Broadcast raw log entry
            await manager.broadcast({
                "type": "log",
                "data": parsed_log,
                "metrics": window_metrics,
                "baseline": baseline_status,
            })

            # Broadcast anomaly alert if detected
            if anomaly_data:
                logger.warning(
                    "ANOMALY DETECTED [%s] Z: %.2f | Rate: %.2f/min | Log: %s",
                    anomaly_data["severity"],
                    anomaly_data["z_score"],
                    anomaly_data["current_error_rate"],
                    anomaly_data["triggering_log"][:80],
                )
                await manager.broadcast({
                    "type": "anomaly",
                    "data": anomaly_data,
                    "baseline": baseline_status,
                })

                # Hand pending AWS delivery to the background dispatcher.
                dispatch_state = anomaly_data.get("aws_dispatch") or {}
                if dispatch_state.get("pending"):
                    try:
                        aws_dispatch_queue.put_nowait(anomaly_data)
                    except asyncio.QueueFull:
                        logger.warning("AWS dispatch queue full; alert %s retained locally only.",
                                       anomaly_data.get("alert_id"))
    except asyncio.CancelledError:
        logger.info("Log tailing worker stopped.")
    except Exception as e:
        logger.error("Unexpected error in log tailing worker: %s", e, exc_info=True)


async def metrics_heartbeat_worker() -> None:
    """Periodically broadcast window metrics and baseline status (every 1.5s)."""
    try:
        while True:
            await asyncio.sleep(1.5)
            if manager.active_connections:
                metrics = detector.get_window_metrics()
                baseline = detector.get_baseline_status()
                z_score, _, _ = detector.calculate_z_score(metrics["error_rate_per_min"])
                await manager.broadcast({
                    "type": "heartbeat",
                    "data": {
                        "metrics": metrics,
                        "baseline": baseline,
                        "z_score": round(z_score, 2),
                    },
                })
    except asyncio.CancelledError:
        pass


async def aws_dispatch_worker() -> None:
    """
    Serially drains the AWS dispatch queue, performing blocking boto3 calls in a
    worker thread (asyncio.to_thread) so the event loop never blocks on AWS.
    Broadcasts an `aws_dispatch_update` frame so the dashboard can flip the alert
    card badge from "dispatching" to its final state.
    """
    try:
        while True:
            anomaly = await aws_dispatch_queue.get()
            try:
                report = await asyncio.to_thread(detector.push_aws_alert, anomaly)
            except Exception as e:
                logger.error("AWS dispatch worker error (alert retained locally): %s", e, exc_info=True)
                report = {
                    "attempted": True,
                    "sns_published": False,
                    "cloudwatch_logged": False,
                    "fallback_active": True,
                    "error": str(e),
                    "fallback_notice": "Fallback: Alert retained locally (dispatcher error).",
                }

            anomaly["aws_dispatch"] = report
            await manager.broadcast({
                "type": "aws_dispatch_update",
                "data": {
                    "alert_id": anomaly.get("alert_id"),
                    "severity": anomaly.get("severity"),
                    "aws_dispatch": report,
                },
            })
            aws_dispatch_queue.task_done()
    except asyncio.CancelledError:
        logger.info("AWS dispatch worker stopped.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage lifecycle of background tailing, dispatch, and metric tasks."""
    tail_task = asyncio.create_task(log_tailing_worker())
    heartbeat_task = asyncio.create_task(metrics_heartbeat_worker())
    dispatch_task = asyncio.create_task(aws_dispatch_worker())
    yield
    tail_task.cancel()
    heartbeat_task.cancel()
    dispatch_task.cancel()
    await asyncio.gather(tail_task, heartbeat_task, dispatch_task, return_exceptions=True)


app = FastAPI(
    title="Real-Time Log Anomaly Detector",
    description="Real-Time log streaming, sliding window error detection, baseline calibration, and AWS alerting.",
    version="1.2.0",
    lifespan=lifespan,
)


@app.get("/", response_class=FileResponse)
async def serve_index():
    """Serves the static index.html dashboard."""
    if os.path.exists(STATIC_HTML_PATH):
        return FileResponse(STATIC_HTML_PATH)
    return HTMLResponse("<h3>index.html not found</h3>", status_code=404)


@app.get("/health")
async def health_check():
    """Health check endpoint providing current detector statistics and baseline status."""
    metrics = detector.get_window_metrics()
    baseline = detector.get_baseline_status()
    z_score, _, _ = detector.calculate_z_score(metrics["error_rate_per_min"])
    return {
        "status": "healthy",
        "active_connections": len(manager.active_connections),
        "log_file": LOG_FILE_PATH,
        "metrics": metrics,
        "baseline": baseline,
        "current_z_score": round(z_score, 2),
        "aws_sns_configured": bool(detector.sns_topic_arn),
        "aws_cloudwatch_configured": bool(detector.cloudwatch_group),
        "aws_fallback_mode": detector.aws_fallback_mode,
        "aws_dispatch_queue_depth": aws_dispatch_queue.qsize(),
    }


@app.post("/api/force-baseline")
async def force_baseline():
    """Endpoint allowing the frontend or presenter to instantly mark baseline as established."""
    detector.force_establish_baseline()
    status = detector.get_baseline_status()
    await manager.broadcast({"type": "baseline_updated", "data": status})
    return JSONResponse(content={"status": "success", "baseline": status})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint streaming raw logs, anomaly alerts, and telemetry to connected clients.
    """
    await manager.connect(websocket)
    try:
        # Send initial status payload
        metrics = detector.get_window_metrics()
        baseline = detector.get_baseline_status()
        z_score, _, _ = detector.calculate_z_score(metrics["error_rate_per_min"])
        await websocket.send_text(json.dumps({
            "type": "init",
            "data": {
                "window_seconds": detector.window_seconds,
                "baseline": baseline,
                "current_z_score": round(z_score, 2),
                "metrics": metrics,
                "aws_sns_configured": bool(detector.sns_topic_arn),
                "aws_cloudwatch_configured": bool(detector.cloudwatch_group),
                "aws_fallback_mode": detector.aws_fallback_mode,
            }
        }))

        # Keep connection alive and process incoming messages
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
            elif data == "force_baseline":
                detector.force_establish_baseline()
                status = detector.get_baseline_status()
                await manager.broadcast({"type": "baseline_updated", "data": status})
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.warning("WebSocket error: %s", e)
        await manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
