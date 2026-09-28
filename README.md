# Real-Time Log Anomaly Detector with Alert Feed (Hackathon Edition)

An enterprise-grade, real-time log anomaly detection engine and observability dashboard built with **Python**, **FastAPI**, **WebSockets**, and **AWS (SNS & CloudWatch Logs)**.

---

## 🏗️ Architecture & Requirements Compliance

1. **Continuous & Efficient Log Monitoring**:
   - `tail_log_file` asynchronous generator mimics `tail -f` without blocking the asyncio loop.
   - Buffers partial lines, guarantees newline completeness, and handles file truncation/rotation.
2. **Rolling Error Rates with Sliding Window**:
   - Uses `collections.deque` with a 60-second sliding window to calculate real-time error counts, error rate per minute, and error percentage with $O(1)$ evictions.
3. **Baseline Establishment Before Alerting**:
   - Explicit baseline calibration state (`min_baseline_samples=15`).
   - Learns normal error rates ($\mu$) and standard deviation ($\sigma$) during baseline observation.
   - Prevents false-positive alert spikes during initialization.
4. **Statistical Deviation Detection (Z-Score)**:
   - Evaluates deviation against learned baseline:
     $$Z = \frac{\text{Current Rolling Error Rate} - \mu}{\sigma}$$
   - Employs minimum variance floor ($\sigma \ge 0.5$) to prevent division by zero or inflated scores.
5. **Severity Assignment**:
   - `WARNING`: $2.0 \le Z < 3.0$
   - `HIGH`: $3.0 \le Z < 4.5$
   - `CRITICAL`: $Z \ge 4.5$
6. **Presentation-Ready Real-Time Frontend (`index.html`)**:
   - Dark-mode split-screen dashboard with glassmorphism styling.
   - **Left Panel**: Raw log stream with auto-scroll, search filter, and level tags.
   - **Right Panel**: Color-coded alert cards (Amber for WARNING, Orange for HIGH, Red for CRITICAL).
   - **Header KPI Bar**: Real-time Z-score deviation, rolling rate, sliding window stats, and baseline progress.
   - **Web Audio Alert Chimes**: Synthesizes audible alarm frequencies on Critical/High alerts.
7. **AWS Alerting with Resilient Fallback Handling**:
   - Pushes JSON alert payloads to AWS SNS topic and/or AWS CloudWatch Logs.
   - Complete fallback error handling: catches `NoCredentialsError`, `PartialCredentialsError`, `ClientError`, and network errors, activating local fallback without crashing the pipeline.
8. **Mock Scenario Demonstrating Baseline-to-Deviation Transition**:
   - `log_generator.py` includes a 5-phase structured demonstration:
     1. Baseline Calibration
     2. Baseline Lock & Confirmation
     3. Mild Deviation (`WARNING`)
     4. Severe Catastrophic Surge (`HIGH` & `CRITICAL` with AWS dispatch)
     5. Self-Healing Recovery

---

## 📁 Project Structure

```
├── app.log              # Target log file monitored in real time
├── detector.py          # LogAnomalyDetector (deque, baseline calibration, Z-score, AWS SNS/CW)
├── index.html           # Presentation-ready dark-mode split-screen UI
├── log_generator.py     # Mock generator with 5-phase baseline-to-outage demo scenario
├── main.py              # FastAPI server, async tail generator, WebSocket (/ws) endpoint
├── README.md            # System documentation
└── requirements.txt     # Python dependencies (fastapi, uvicorn, websockets, boto3)
```

---

## 🚀 Running the Project

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AWS 
If you wish to stream alerts to an AWS SNS topic or CloudWatch Logs:
```powershell
$env:AWS_SNS_TOPIC_ARN = "Your topic"
$env:AWS_CLOUDWATCH_GROUP = "/aws/events/log-anomalies"
$env:AWS_DEFAULT_REGION = "Your region"
```
*(If credentials are unconfigured or invalid, the detector automatically falls back to local simulation mode).*

### 3. Start the FastAPI Server
```bash
uvicorn main:app --reload --port 8000
```
Open **[http://localhost:8000](http://localhost:8000)** in your browser.

### 4. Run the Hackathon Demonstration
In a second terminal, run the 5-phase demo walkthrough:
```bash
python log_generator.py --demo
```
To run in continuous traffic mode:
```bash
python log_generator.py --continuous
```
To trigger an immediate error burst:
```bash
python log_generator.py --burst-now --burst-size 20
```

---

## 📡 API & WebSocket Reference

- `GET /`: Serves the real-time split-screen dashboard.
- `GET /health`: Returns detector metrics, baseline status, and AWS connection state.
- `POST /api/force-baseline`: Manually locks and confirms baseline (useful for instant presentations).
- `WebSocket /ws`: Streams raw logs, telemetry metrics, and anomaly events.
