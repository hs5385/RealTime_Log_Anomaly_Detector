# AWS Setup Guide (SNS + CloudWatch Logs)

End-to-end steps to move the detector from **Local Fallback Simulation Mode** to **live AWS alerting**.

> Everything is optional. If credentials or ARNs are missing/invalid, the detector keeps
> running and retains alerts locally — the pipeline never crashes.

---

## 1. Prerequisites

```powershell
pip install -r requirements.txt
aws --version          # AWS CLI v2
aws configure          # or export credentials via env vars (see step 4)
```

During `aws configure` you will be asked for:

| Prompt | Example |
| :--- | :--- |
| AWS Access Key ID | `AKIA...` |
| AWS Secret Access Key | `...` |
| Default region | `us-east-1` |
| Default output format | `json` |

> ⚠️ Use a **least-privilege IAM policy**, never your root account keys.

---

## 2. Create the SNS Topic (email alerts in ~2 minutes)

```powershell
$AWS_REGION = "us-east-1"

# Create topic
aws sns create-topic --name log-anomaly-alerts --region $AWS_REGION
# -> returns {"TopicArn": "arn:aws:sns:us-east-1:123456789012:log-anomaly-alerts"}

$env:AWS_SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:log-anomaly-alerts"

# Subscribe your email
aws sns subscribe `
  --topic-arn $env:AWS_SNS_TOPIC_ARN `
  --protocol email `
  --notification-endpoint you@example.com

# Confirm the subscription from the email AWS sends you (one time only)
```

Optional: subscribe an HTTPS endpoint for Slack/Discord webhooks via `aws sns subscribe --protocol https`.

---

## 3. Create the CloudWatch Log Group & Stream

The detector now auto-creates both on first dispatch, but you can pre-create them:

```powershell
aws logs create-log-group --log-group-name /aws/events/log-anomalies --region $AWS_REGION

aws logs create-log-stream `
  --log-group-name /aws/events/log-anomalies `
  --log-stream-name detector-alerts
```

Query alerts later with **CloudWatch Logs Insights**:

```
fields @timestamp, @message
| filter @message like /"severity"/
| sort @timestamp desc
| limit 50
```

---

## 4. Environment Variables

```powershell
# Credentials (skip if you already ran `aws configure` or use an instance/task role)
$env:AWS_ACCESS_KEY_ID     = "AKIA..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN      = "..."   # only for temporary/STS credentials

# Detector targets
$env:AWS_DEFAULT_REGION    = "us-east-1"
$env:AWS_SNS_TOPIC_ARN     = "arn:aws:sns:us-east-1:123456789012:log-anomaly-alerts"
$env:AWS_CLOUDWATCH_GROUP  = "/aws/events/log-anomalies"
$env:AWS_CLOUDWATCH_STREAM = "detector-alerts"    # optional, this is the default

# Optional tuning
$env:MIN_BASELINE_SAMPLES  = "15"
$env:DETECTOR_WINDOW_SECONDS = "60"
```

**PowerShell only (persists for the current session):** run these before `uvicorn main:app`.

**Minimal IAM policy** attach to the user/role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "arn:aws:sns:us-east-1:123456789012:log-anomaly-alerts"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/log-anomalies:*"
    }
  ]
}
```

---

## 5. Run & Verify

```powershell
uvicorn main:app --reload --port 8000
```

Check connection state:

```powershell
curl http://localhost:8000/health
```

Expected when wired up:

```json
{
  "status": "healthy",
  "aws_sns_configured": true,
  "aws_cloudwatch_configured": true,
  "aws_fallback_mode": false,
  "aws_dispatch_queue_depth": 0
}
```

Then trigger the demo surge:

```powershell
python log_generator.py --demo
```

Verify each channel:

| Channel | How to verify |
| :--- | :--- |
| **Dashboard** | Alert card shows `⏳ AWS Dispatching…` then flips to `🚀 AWS Alert Pushed (<id>)` |
| **SNS** | Confirmation email arrives within seconds of a HIGH/CRITICAL alert |
| **CloudWatch** | Log group `/aws/events/log-anomalies` contains the JSON alert payload |
| **`/health`** | `aws_fallback_mode: false`, `aws_dispatch_queue_depth: 0` |

---

## 6. Troubleshooting

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| `🛡️ AWS Fallback Active (Local Mode)` | No ARNs set | Set `AWS_SNS_TOPIC_ARN` / `AWS_CLOUDWATCH_GROUP` |
| `NoCredentialsError` | No keys / no role | `aws configure` or export `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` |
| `AccessDenied` / `AuthorizationError` | IAM policy | Attach the policy in section 4; confirm the SNS subscription |
| `ResourceNotFoundException` on logs | Group/stream missing | Detector auto-creates now — or pre-create (section 3) |
| `InvalidEndpointException` | Wrong region | Match `AWS_DEFAULT_REGION` to the topic's region |
| Stuck `⏳ AWS Dispatching…` | Network timeout / queue backlog | Check `aws_dispatch_queue_depth` in `/health` and AWS region connectivity |

---

## 7. How It Works Internally

```
log line ──► tail_log_file ──► detector.process_log(aws_dispatch=False)
                                     │
                                     ├─► WebSocket broadcast "anomaly" (badge: ⏳ Dispatching…)
                                     │
                                     └─► aws_dispatch_queue (bounded, 250)
                                              │
                                              ▼
                                   aws_dispatch_worker  ── asyncio.to_thread(push_aws_alert)
                                              │
                                              ├─► SNS publish
                                              ├─► CloudWatch create group/stream + put_log_events
                                              ▼
                                   WebSocket broadcast "aws_dispatch_update"
                                              │
                                              ▼
                                   dashboard badge flips to final state
```

Blocking `boto3` calls run in a worker thread, so log tailing and the dashboard
stream keep flowing even if AWS is slow or unreachable. The cooldown slot is
reserved *before* the network call, so a burst cannot produce duplicate sends.
