"""
Mock Log Generator with Hackathon Demo Scenario.
Simulates production microservice logs and clearly demonstrates the full lifecycle:
1. Baseline Establishment (steady INFO/DEBUG traffic)
2. Mild Deviation (Warning alert)
3. Severe Outage Spike (Critical alert & AWS dispatch)
4. Recovery & Eviction back to baseline
"""

import argparse
from datetime import datetime
import os
import random
import sys
import time

# Ensure clean UTF-8 console output across all Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

LOG_FILE = os.getenv("LOG_FILE_PATH", "app.log")

SERVICES = [
    "auth-service",
    "payment-gateway",
    "order-processor",
    "inventory-api",
    "database-cluster",
    "redis-cache",
    "api-gateway",
    "notification-worker",
]

NORMAL_LOGS = [
    ("INFO", "User session verified: user_id={user_id} token_type=Bearer"),
    ("INFO", "Order #{order_id} status updated to PROCESSING"),
    ("DEBUG", "Cache lookup for key 'user_profile:{user_id}' - HIT (2.1ms)"),
    ("INFO", "Payment intent confirmed: id=pi_{order_id} amount=${amount}"),
    ("DEBUG", "Database query execution: SELECT * FROM items WHERE sku='{sku}' - 12ms"),
    ("INFO", "Notification dispatched to user_{user_id}@example.com via SES"),
    ("DEBUG", "Health check probe OK from 10.0.4.12: latency 1.4ms"),
    ("INFO", "Inventory reservation confirmed for SKU: {sku} qty=1"),
    ("DEBUG", "Garbage collection completed: reclaimed 142MB in 8.2ms"),
    ("INFO", "HTTP 200 OK: GET /api/v2/catalog response_time=18ms"),
]

MILD_ERROR_LOGS = [
    ("WARNING", "Slow query detected on database-replica: execution took 1240ms"),
    ("ERROR", "Transient socket timeout contacting inventory-service; retrying attempt 1/3"),
    ("WARNING", "Memory utilization warning: worker process heap at 78%"),
    ("ERROR", "OAuth token refresh failed for user_id={user_id}; prompting re-login"),
]

SEVERE_BURST_LOGS = [
    ("CRITICAL", "Payment gateway upstream timeout: 504 Gateway Timeout from payment-provider-relay"),
    ("ERROR", "Database connection pool exhausted: 100/100 active connections in use"),
    ("CRITICAL", "Cascading circuit breaker TRIPPED for order-processor: failure rate 82%"),
    ("ERROR", "Transaction deadlock detected while processing order #{order_id}; rolled back"),
    ("CRITICAL", "Out of memory alert: Heap usage reached 98.4% (threshold: 90%)"),
    ("ERROR", "Redis connection refused: ConnectionResetError(10054, 'Host unreachable')"),
    ("CRITICAL", "Security event: potential brute-force authentication spike detected for user_{user_id}"),
]


def format_log_entry(level: str, service: str, message_template: str) -> str:
    """Format log line with ISO timestamp, level, and service component."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    user_id = random.randint(1000, 9999)
    order_id = random.randint(100000, 999999)
    amount = f"{random.uniform(10.0, 450.0):.2f}"
    sku = f"SKU-{random.randint(100, 999)}"

    msg = message_template.format(
        user_id=user_id,
        order_id=order_id,
        amount=amount,
        sku=sku,
        service=service,
    )
    return f"{now_str} [{level}] [{service}] {msg}"


def write_log(file_path: str, log_line: str) -> None:
    """Appends log entry to target file with immediate disk flush."""
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")
        f.flush()


def inject_burst(file_path: str, burst_size: int = 15) -> None:
    """Injects a sudden burst of severe error/critical logs."""
    print(f"\n[!] >>> INJECTING CATASTROPHIC BURST ({burst_size} logs) <<< [!]")
    for i in range(burst_size):
        level, template = random.choice(SEVERE_BURST_LOGS)
        service = random.choice(["database-cluster", "payment-gateway", "order-processor", "redis-cache"])
        line = format_log_entry(level, service, template)
        write_log(file_path, line)
        print(f"  [{level}] {line}")
        time.sleep(random.uniform(0.06, 0.14))
    print("[!] >>> BURST INJECTION COMPLETE <<<\n")


def run_demo_scenario(file_path: str) -> None:
    """
    Executes a structured 4-phase hackathon demonstration:
    Phase 1: Establishing Baseline (normal traffic)
    Phase 2: Baseline Confirmed (steady state)
    Phase 3: Mild Deviation (Warning alert triggered)
    Phase 4: Severe Anomaly Surge (Critical alert & AWS dispatch triggered)
    Phase 5: Self-Healing Recovery (window evicts burst, returning to baseline)
    """
    print("\n" + "=" * 78)
    print(">> STARTING HACKATHON DEMO: BASELINE TO SEVERE ANOMALY DEVIATION")
    print(f"-> Target Log File: {os.path.abspath(file_path)}")
    print("=" * 78 + "\n")

    # PHASE 1: Baseline Establishment
    print("[PHASE 1/5] ESTABLISHING BASELINE (Normal Operations)")
    print("-> Emitting 18 normal INFO/DEBUG logs to calibrate baseline mean and std dev...")
    for i in range(18):
        level, template = random.choice(NORMAL_LOGS)
        service = random.choice(SERVICES)
        line = format_log_entry(level, service, template)
        write_log(file_path, line)
        print(f"  [SAMPLE {i+1:02d}/18] {line}")
        time.sleep(0.35)

    print("\n[PHASE 2/5] BASELINE ESTABLISHED & CONFIRMED")
    print("-> Normal baseline is now calibrated. Anomaly detector is armed.\n")
    time.sleep(1.5)

    # PHASE 3: Mild Deviation
    print("[PHASE 3/5] INJECTING MILD DEVIATION (Expected: WARNING Alert)")
    for i in range(3):
        level, template = random.choice(MILD_ERROR_LOGS)
        service = random.choice(["database-cluster", "inventory-api"])
        line = format_log_entry(level, service, template)
        write_log(file_path, line)
        print(f"  [MILD ANOMALY] {line}")
        time.sleep(0.5)

    print("-> Mild deviation registered. Pausing 3 seconds...\n")
    time.sleep(3.0)

    # PHASE 4: Severe Outage Spike
    print("[PHASE 4/5] INJECTING SEVERE OUTAGE SURGE (Expected: HIGH -> CRITICAL + AWS Alert)")
    inject_burst(file_path, burst_size=16)

    # PHASE 5: Recovery
    print("[PHASE 5/5] SELF-HEALING & RECOVERY PHASE")
    print("-> Resuming normal traffic. Watch sliding window error rate decay back to baseline...")
    for i in range(15):
        level, template = random.choice(NORMAL_LOGS)
        service = random.choice(SERVICES)
        line = format_log_entry(level, service, template)
        write_log(file_path, line)
        print(f"  [RECOVERY] {line}")
        time.sleep(0.5)

    print("\n" + "=" * 78)
    print(">> DEMO WALKTHROUGH COMPLETED SUCCESSFULLY!")
    print("=" * 78 + "\n")


def run_continuous_traffic(file_path: str, burst_interval: float = 20.0, burst_size: int = 14) -> None:
    """Continuous simulation loop."""
    print(">> Mock Log Generator Running in Continuous Mode...")
    print(f"-> Target: {file_path} | Bursts every {burst_interval}s")
    last_burst = time.time()
    try:
        while True:
            now = time.time()
            if now - last_burst >= burst_interval:
                inject_burst(file_path, burst_size)
                last_burst = time.time()
                continue

            level, template = random.choice(NORMAL_LOGS)
            service = random.choice(SERVICES)
            line = format_log_entry(level, service, template)
            write_log(file_path, line)
            print(f"[NORMAL] {line}")
            time.sleep(random.uniform(0.3, 0.7))
    except KeyboardInterrupt:
        print("\nGenerator stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Generator with Baseline-to-Deviation demo.")
    parser.add_argument("--file", default=LOG_FILE, help="Path to destination log file (default: app.log)")
    parser.add_argument("--demo", action="store_true", help="Run the scripted 5-phase hackathon walkthrough")
    parser.add_argument("--continuous", action="store_true", help="Run infinite continuous traffic")
    parser.add_argument("--burst-now", action="store_true", help="Immediately inject one severe burst")
    parser.add_argument("--burst-size", type=int, default=15, help="Number of logs in burst")

    args = parser.parse_args()

    if args.burst_now:
        inject_burst(args.file, burst_size=args.burst_size)
    elif args.continuous:
        run_continuous_traffic(args.file)
    else:
        # Default behavior: run the presentation demo scenario
        run_demo_scenario(args.file)
