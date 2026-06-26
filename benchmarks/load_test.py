import time
import threading
import urllib.request
import json
import random

TARGET_RPS = 8        # reduce from 17
DURATION = 120        # 2 minutes
sent = 0
errors = 0
latencies = []
lock = threading.Lock()

def send_request():
    global sent, errors
    payload = json.dumps({
        "user_id": f"USR{random.randint(1000, 9999)}",
        "amount": random.choice([
            round(random.uniform(50, 5000), 2),
            round(random.uniform(50000, 200000), 2)
        ]),
        "is_new_merchant": random.random() < 0.1,
        "merchant_category": random.choice(["ecommerce","food","unknown"]),
        "payment_method": random.choice(["UPI","CARD"])
    }).encode('utf-8')

    try:
        start = time.time()
        req = urllib.request.Request(
            'http://127.0.0.1:8002/score',
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        urllib.request.urlopen(req, timeout=2)  # reduce from 5
        latency_ms = (time.time() - start) * 1000
        with lock:
            sent += 1
            latencies.append(latency_ms)
    except Exception as e:
        with lock:
            errors += 1

def percentile(data, p):
    if not data: return 0
    s = sorted(data)
    return round(s[int(len(s) * p / 100)], 2)

def main():
    print(f"Load test: {TARGET_RPS} RPS for {DURATION}s")
    start = time.time()

    while time.time() - start < DURATION:
        threads = [threading.Thread(target=send_request) for _ in range(TARGET_RPS)]
        for t in threads: t.start()
        for t in threads: t.join()
        elapsed = round(time.time() - start)
        if elapsed > 0:
            print(f"[{elapsed}s] Sent={sent} | Errors={errors} | RPM={round(sent/(elapsed/60))}")
        time.sleep(1)

    print(f"\n{'='*40}")
    print(f"Total sent:    {sent}")
    print(f"Total errors:  {errors}")
    print(f"Error rate:    {round(errors/(sent+errors)*100,2)}%")
    print(f"Effective RPM: {round(sent/DURATION*60)}")
    print(f"p50 latency:   {percentile(latencies,50)}ms")
    print(f"p95 latency:   {percentile(latencies,95)}ms")
    print(f"p99 latency:   {percentile(latencies,99)}ms")
    print(f"{'='*40}")

if __name__ == "__main__":
    main()