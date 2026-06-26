import urllib.request
import json
import threading

# Send same transaction 100 times concurrently
SAME_TXN_PAYLOAD = json.dumps({
    "user_id": "USR-IDEM-TEST",
    "amount": 5000.0,
    "is_new_merchant": False,
    "merchant_category": "ecommerce"
}).encode('utf-8')

results = []
lock = threading.Lock()

def send():
    req = urllib.request.Request(
        'http://127.0.0.1:8002/score',
        data=SAME_TXN_PAYLOAD,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    resp = json.loads(urllib.request.urlopen(req).read())
    with lock:
        results.append(resp['decision'])

threads = [threading.Thread(target=send) for _ in range(100)]
for t in threads: t.start()
for t in threads: t.join()

unique_decisions = set(results)
print(f"Sent 100 concurrent requests")
print(f"Total responses: {len(results)}")
print(f"Unique decisions: {unique_decisions}")
print(f"All consistent: {len(unique_decisions) == 1}")