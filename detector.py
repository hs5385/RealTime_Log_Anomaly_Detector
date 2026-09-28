"""
Log Anomaly Detector Engine.
Maintains a rolling sliding window with collections.deque to calculate real-time error rates.
Establishes a baseline for normal behavior before triggering alerts, detects statistical
deviations using Z-score logic, assigns severities (WARNING, HIGH, CRITICAL), and pushes
alerts to AWS SNS / CloudWatch with robust fallback error handling.
"""

from collections import deque
from datetime import datetime
import json
import logging
import math
import os
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple

# Graceful Boto3 import with comprehensive AWS error handling
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None
    BOTO3_AVAILABLE = False
    BotoCoreError = ClientError = NoCredentialsError = PartialCredentialsError = Exception

logger = logging.getLogger("LogAnomalyDetector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class LogAnomalyDetector:
    """
    Real-Time Log Anomaly Detector.
    
    Features:
    - Sliding window rolling error rate calculation (collections.deque)
    - Baseline calibration and establishment phase before triggering alerts
    - Z-Score deviation computation against calibrated baseline mean and std dev
    - Severity mapping: WARNING (Z >= 2.0), HIGH (Z >= 3.0), CRITICAL (Z >= 4.5)
    - AWS SNS & CloudWatch Logs alert dispatch with automatic fallback error handling
    """

    LEVEL_PATTERN = re.compile(r"\b(CRITICAL|FATAL|ERROR|WARN(?:ING)?|INFO|DEBUG|TRACE)\b", re.IGNORECASE)
    ERROR_LEVELS = {"CRITICAL", "FATAL", "ERROR"}

    def __init__(
        self,
        window_seconds: float = 60.0,
        min_baseline_samples: int = 15,
        default_baseline_mean: float = 1.5,
        default_baseline_std: float = 1.0,
        min_std: float = 0.5,
        sns_topic_arn: Optional[str] = None,
        cloudwatch_group: Optional[str] = None,
        cloudwatch_stream: Optional[str] = None,
        aws_region: Optional[str] = None,
        sns_cooldown_seconds: float = 5.0,
        auto_establish_baseline: bool = False,
    ) -> None:
        self.window_seconds = float(window_seconds)
        self.min_baseline_samples = int(min_baseline_samples)
        self.min_std = float(min_std)
        self.sns_cooldown_seconds = float(sns_cooldown_seconds)

        # Baseline state
        self.baseline_established: bool = auto_establish_baseline
        self.baseline_samples: deque = deque(maxlen=500)
        self.learned_mean: float = float(default_baseline_mean)
        self.learned_std: float = float(default_baseline_std)

        # Sliding window of logs: deque of (timestamp, is_error, raw_line, level)
        self.window: deque = deque()

        # AWS Configuration
        self.sns_topic_arn = sns_topic_arn or os.getenv("AWS_SNS_TOPIC_ARN", "")
        self.cloudwatch_group = cloudwatch_group or os.getenv("AWS_CLOUDWATCH_GROUP", "")
        self.cloudwatch_stream = cloudwatch_stream or os.getenv("AWS_CLOUDWATCH_STREAM", "detector-alerts")
        self.aws_region = aws_region or os.getenv("AWS_DEFAULT_REGION", "us-east-1")

        # Throttling & AWS status
        self._last_alert_time: float = 0.0
        self._last_alert_severity: Optional[str] = None
        self._aws_sns_client = None
        self._aws_cw_client = None
        self.aws_fallback_mode: bool = False
        self.aws_last_error: Optional[str] = None

        # Thread-safety: AWS dispatch may run off the event loop in a worker thread,
        # while /health and the WebSocket heartbeat read client state concurrently.
        self._aws_client_lock = threading.Lock()
        self._cw_stream_ready: bool = False

    def _get_sns_client(self):
        """Lazily initialize AWS SNS client with credential validation (thread-safe)."""
        if not BOTO3_AVAILABLE:
            return None
        if self._aws_sns_client is None and not self.aws_fallback_mode:
            with self._aws_client_lock:
                if self._aws_sns_client is None and not self.aws_fallback_mode:
                    try:
                        self._aws_sns_client = boto3.client("sns", region_name=self.aws_region)
                    except Exception as e:
                        self.aws_fallback_mode = True
                        self.aws_last_error = f"SNS client initialization failed: {e}"
                        logger.warning(self.aws_last_error)
                        return None
        return self._aws_sns_client

    def _get_cloudwatch_client(self):
        """Lazily initialize AWS CloudWatch Logs client with credential validation (thread-safe)."""
        if not BOTO3_AVAILABLE:
            return None
        if self._aws_cw_client is None and not self.aws_fallback_mode:
            with self._aws_client_lock:
                if self._aws_cw_client is None and not self.aws_fallback_mode:
                    try:
                        self._aws_cw_client = boto3.client("logs", region_name=self.aws_region)
                    except Exception as e:
                        self.aws_fallback_mode = True
                        self.aws_last_error = f"CloudWatch client initialization failed: {e}"
                        logger.warning(self.aws_last_error)
                        return None
        return self._aws_cw_client

    def _ensure_log_stream(self, cw_client) -> bool:
        """
        Idempotently create the CloudWatch Logs group and stream before the first
        put_log_events call. Without this, CloudWatch dispatch always fails with
        ResourceNotFoundException and silently drops into local fallback mode.
        """
        if self._cw_stream_ready:
            return True
        if not cw_client:
            return False

        try:
            cw_client.create_log_group(logGroupName=self.cloudwatch_group)
            logger.info("Created CloudWatch Log Group: %s", self.cloudwatch_group)
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code not in ("ResourceAlreadyExistsException",):
                logger.warning("Could not create CloudWatch Log Group %s: %s", self.cloudwatch_group, e)
                return False

        try:
            cw_client.create_log_stream(
                logGroupName=self.cloudwatch_group,
                logStreamName=self.cloudwatch_stream,
            )
            logger.info("Created CloudWatch Log Stream: %s", self.cloudwatch_stream)
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code not in ("ResourceAlreadyExistsException",):
                logger.warning("Could not create CloudWatch Log Stream %s: %s", self.cloudwatch_stream, e)
                return False

        self._cw_stream_ready = True
        return True

    def parse_log_line(self, log_line: str) -> Dict[str, Any]:
        """Parse raw log string into structured metadata."""
        match = self.LEVEL_PATTERN.search(log_line)
        level = match.group(1).upper() if match else "INFO"
        if level == "WARN":
            level = "WARNING"
        is_error = level in self.ERROR_LEVELS
        return {
            "level": level,
            "is_error": is_error,
            "raw": log_line.strip(),
        }

    def _evict_expired(self, current_time: float) -> None:
        """Evict records older than window_seconds from the sliding window."""
        cutoff_time = current_time - self.window_seconds
        while self.window and self.window[0][0] < cutoff_time:
            self.window.popleft()

    def get_window_metrics(self, current_time: Optional[float] = None) -> Dict[str, Any]:
        """
        Calculates rolling error metrics over the active sliding window:
        - Total logs in window
        - Error logs count in window
        - Rolling error rate (errors per minute)
        - Error percentage
        """
        now = current_time if current_time is not None else time.time()
        self._evict_expired(now)

        total_logs = len(self.window)
        error_count = sum(1 for _, is_error, _, _ in self.window if is_error)

        effective_window = max(self.window_seconds, 1.0)
        error_rate_per_min = (error_count / effective_window) * 60.0
        error_pct = (error_count / total_logs * 100.0) if total_logs > 0 else 0.0

        return {
            "total_logs": total_logs,
            "error_count": error_count,
            "error_rate_per_min": round(error_rate_per_min, 2),
            "error_percentage": round(error_pct, 2),
            "window_seconds": self.window_seconds,
        }

    def establish_baseline_sample(self, error_rate: float) -> bool:
        """
        Accumulates baseline samples during normal operation.
        Returns True once baseline is established.
        """
        self.baseline_samples.append(error_rate)

        if not self.baseline_established and len(self.baseline_samples) >= self.min_baseline_samples:
            rates = list(self.baseline_samples)
            self.learned_mean = sum(rates) / len(rates)
            variance = sum((r - self.learned_mean) ** 2 for r in rates) / len(rates)
            self.learned_std = max(math.sqrt(variance), self.min_std)
            self.baseline_established = True
            logger.info(
                "=== BASELINE ESTABLISHED === Mean: %.2f err/min | StdDev: %.2f | Samples: %d",
                self.learned_mean,
                self.learned_std,
                len(rates),
            )
        elif self.baseline_established and len(self.baseline_samples) % 10 == 0:
            # Gradually update baseline with recent non-anomalous samples
            rates = list(self.baseline_samples)[-100:]
            self.learned_mean = sum(rates) / len(rates)
            variance = sum((r - self.learned_mean) ** 2 for r in rates) / len(rates)
            self.learned_std = max(math.sqrt(variance), self.min_std)

        return self.baseline_established

    def force_establish_baseline(self, mean: Optional[float] = None, std: Optional[float] = None) -> None:
        """Manually marks the baseline as established (useful for fast-track testing/demo)."""
        if mean is not None:
            self.learned_mean = float(mean)
        if std is not None:
            self.learned_std = max(float(std), self.min_std)
        self.baseline_established = True
        logger.info("Baseline manually confirmed: Mean=%.2f, Std=%.2f", self.learned_mean, self.learned_std)

    def get_baseline_status(self) -> Dict[str, Any]:
        """Returns baseline health, calibration progress, and statistical parameters."""
        samples_collected = len(self.baseline_samples)
        progress_pct = min(100.0, round((samples_collected / max(self.min_baseline_samples, 1)) * 100.0, 1))
        return {
            "established": self.baseline_established,
            "status": "READY" if self.baseline_established else "CALIBRATING",
            "samples_collected": samples_collected,
            "min_samples_required": self.min_baseline_samples,
            "progress_percent": progress_pct if not self.baseline_established else 100.0,
            "mean": round(self.learned_mean, 2),
            "std": round(self.learned_std, 2),
        }

    def calculate_z_score(self, current_error_rate: float) -> Tuple[float, float, float]:
        """
        Computes Z-Score deviation from calibrated baseline:
        Z = (current_error_rate - mean) / std
        """
        mean = self.learned_mean
        std = max(self.learned_std, self.min_std)
        z_score = (current_error_rate - mean) / std
        return z_score, mean, std

    def determine_severity(self, z_score: float) -> Optional[str]:
        """
        Assigns severity based on statistical standard deviations:
        - Z >= 4.5: CRITICAL
        - Z >= 3.0: HIGH
        - Z >= 2.0: WARNING
        - otherwise: None
        """
        if z_score >= 4.5:
            return "CRITICAL"
        if z_score >= 3.0:
            return "HIGH"
        if z_score >= 2.0:
            return "WARNING"
        return None

    def push_aws_alert(self, anomaly_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pushes alert to AWS SNS topic and/or AWS CloudWatch Logs.
        Provides robust fallback error handling if AWS credentials, permissions, or connectivity fail.
        """
        dispatch_report = {
            "service": "AWS_SNS_CLOUDWATCH",
            "attempted": True,
            "sns_published": False,
            "cloudwatch_logged": False,
            "fallback_active": False,
            "message_id": None,
            "error": None,
            "fallback_notice": None,
        }

        # Check if AWS credentials or endpoints are available
        has_sns = bool(self.sns_topic_arn)
        has_cw = bool(self.cloudwatch_group)

        if not has_sns and not has_cw:
            dispatch_report["fallback_active"] = True
            dispatch_report["fallback_notice"] = (
                "AWS credentials/endpoints unconfigured. Operating in Local Fallback Simulation Mode."
            )
            return dispatch_report

        alert_json = json.dumps(anomaly_payload, indent=2, default=str)
        subject = f"[{anomaly_payload['severity']}] Log Anomaly Alert (Z-Score: {anomaly_payload['z_score']:.2f})"[:100]

        # 1. Publish to AWS SNS
        if has_sns:
            try:
                sns_client = self._get_sns_client()
                if sns_client:
                    response = sns_client.publish(
                        TopicArn=self.sns_topic_arn,
                        Subject=subject,
                        Message=alert_json,
                    )
                    dispatch_report["sns_published"] = True
                    dispatch_report["message_id"] = response.get("MessageId")
                    logger.info("AWS SNS alert dispatched successfully: %s", dispatch_report["message_id"])
                else:
                    raise NoCredentialsError()
            except (NoCredentialsError, PartialCredentialsError) as e:
                dispatch_report["fallback_active"] = True
                dispatch_report["error"] = f"AWS credentials missing or incomplete: {e}"
                dispatch_report["fallback_notice"] = "Fallback: Alert retained locally and broadcast over WebSockets."
                logger.warning("AWS SNS Credential Fallback: %s", dispatch_report["error"])
            except ClientError as e:
                dispatch_report["fallback_active"] = True
                dispatch_report["error"] = f"AWS SNS ClientError ({e.response.get('Error', {}).get('Code')}): {e}"
                dispatch_report["fallback_notice"] = "Fallback: Alert retained locally (AWS permissions/ARN issue)."
                logger.warning("AWS SNS Client Error: %s", dispatch_report["error"])
            except Exception as e:
                dispatch_report["fallback_active"] = True
                dispatch_report["error"] = f"AWS SNS unexpected exception: {e}"
                dispatch_report["fallback_notice"] = "Fallback: Safe execution continued without crashing pipeline."
                logger.error("AWS SNS Dispatch Error: %s", e)

        # 2. Push to AWS CloudWatch Logs (if configured)
        if has_cw:
            try:
                cw_client = self._get_cloudwatch_client()
                if cw_client and self._ensure_log_stream(cw_client):
                    timestamp_ms = int(anomaly_payload.get("timestamp", time.time()) * 1000)
                    event = {"timestamp": timestamp_ms, "message": alert_json}
                    try:
                        cw_client.put_log_events(
                            logGroupName=self.cloudwatch_group,
                            logStreamName=self.cloudwatch_stream,
                            logEvents=[event],
                        )
                    except ClientError as e:
                        # Older accounts still require the sequence token: retry once.
                        code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
                        expected = getattr(e, "response", {}).get("Error", {}).get("Message", "")
                        if code == "InvalidSequenceTokenException" and "sequenceToken" in expected:
                            token = expected.split("sequenceToken is:")[1].strip().strip('"')
                            cw_client.put_log_events(
                                logGroupName=self.cloudwatch_group,
                                logStreamName=self.cloudwatch_stream,
                                sequenceToken=token,
                                logEvents=[event],
                            )
                        else:
                            raise
                    dispatch_report["cloudwatch_logged"] = True
                    logger.info("AWS CloudWatch log event dispatched successfully.")
            except Exception as e:
                dispatch_report["fallback_active"] = True
                self.aws_last_error = f"CloudWatch dispatch failed: {e}"
                logger.warning("AWS CloudWatch Log event dispatch failed (fallback active): %s", e)

        return dispatch_report

    def process_log(
        self,
        log_line: str,
        timestamp: Optional[float] = None,
        aws_dispatch: bool = True,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Ingests a raw log line, updates sliding window, establishes baseline,
        calculates rolling rate and Z-score deviation, and assigns severity.

        Args:
            log_line: Raw log line to ingest.
            timestamp: Optional explicit event timestamp (defaults to now).
            aws_dispatch: When True, SNS/CloudWatch delivery happens inline (blocking).
                When False, the anomaly is marked `pending` and the caller is expected
                to run `push_aws_alert()` in a worker thread, keeping the event loop free.

        Returns:
            Tuple of (parsed_log_dict, anomaly_dict_or_None)
        """
        now = timestamp if timestamp is not None else time.time()
        parsed = self.parse_log_line(log_line)
        parsed["timestamp"] = now

        # 1. Update rolling sliding window (collections.deque)
        self.window.append((now, parsed["is_error"], parsed["raw"], parsed["level"]))

        # 2. Calculate rolling error metrics over sliding window
        metrics = self.get_window_metrics(now)
        error_rate = metrics["error_rate_per_min"]

        # 3. Baseline Calibration & Establishment
        # If baseline is not yet established, accumulate normal rate samples
        if not self.baseline_established:
            # During calibration, normal logs and minor errors establish baseline behavior
            self.establish_baseline_sample(error_rate)
            # Do not trigger anomalies while calibrating normal baseline!
            return parsed, None

        # If baseline IS established, update baseline during normal low-error traffic
        if not parsed["is_error"] and metrics["error_count"] <= 2:
            self.establish_baseline_sample(error_rate)

        # 4. Detect deviations from established baseline
        z_score, mean, std = self.calculate_z_score(error_rate)
        severity = self.determine_severity(z_score)

        anomaly_info = None

        # 5. Appropriately assign severity & generate alert
        if severity and metrics["error_count"] > 0:
            anomaly_info = {
                "alert_id": f"alert-{int(now * 1000)}",
                "timestamp": now,
                "formatted_time": datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S"),
                "severity": severity,
                "z_score": round(z_score, 2),
                "current_error_rate": error_rate,
                "baseline_mean": round(mean, 2),
                "baseline_std": round(std, 2),
                "deviation_delta": round(error_rate - mean, 2),
                "window_errors": metrics["error_count"],
                "window_total_logs": metrics["total_logs"],
                "error_percentage": metrics["error_percentage"],
                "triggering_log": parsed["raw"],
                "log_level": parsed["level"],
                "aws_dispatch": None,
            }

            # 6. AWS Alert Integration for HIGH and CRITICAL severities
            if severity in ("HIGH", "CRITICAL"):
                time_since_last_alert = now - self._last_alert_time
                is_escalation = (self._last_alert_severity != severity)

                if time_since_last_alert >= self.sns_cooldown_seconds or is_escalation:
                    # Reserve the cooldown slot up front so that a burst of logs arriving
                    # while the network call is in flight cannot trigger duplicate sends.
                    self._last_alert_time = now
                    self._last_alert_severity = severity

                    if aws_dispatch:
                        anomaly_info["aws_dispatch"] = self.push_aws_alert(anomaly_info)
                    else:
                        anomaly_info["aws_dispatch"] = {
                            "attempted": True,
                            "pending": True,
                            "sns_published": False,
                            "cloudwatch_logged": False,
                            "fallback_active": False,
                            "message_id": None,
                            "error": None,
                            "fallback_notice": None,
                            "note": "Dispatch deferred to background worker (non-blocking mode).",
                        }
                else:
                    anomaly_info["aws_dispatch"] = {
                        "attempted": False,
                        "fallback_active": False,
                        "reason": f"Throttled by alert cooldown ({self.sns_cooldown_seconds}s)",
                    }

        return parsed, anomaly_info
