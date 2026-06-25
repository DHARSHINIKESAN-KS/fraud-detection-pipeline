import json
import time
import logging
import requests
from datetime import datetime, timezone
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'txn-events',
    bootstrap_servers='localhost:9093',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='clickhouse-consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

CH_URL = "http://localhost:8124/"
BATCH_SIZE = 100
FLUSH_INTERVAL = 5

def insert_batch(batch):
    rows = []
    for r in batch:
        row = "\t".join([
            str(r.get('txn_id','')),
            str(r.get('user_id','')),
            str(float(r.get('amount',0))),
            str(r.get('merchant_id','unknown')),
            str(r.get('merchant_category','unknown')),
            str(r.get('payment_method','unknown')),
            str(r.get('location','unknown')),
            str(float(r.get('risk_score',0.0))),
            str(r.get('decision','ALLOW')),
            str(json.dumps(r.get('reasons',[]))),
            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        ])
        rows.append(row)
    data = "INSERT INTO fraud_detection.transactions FORMAT TabSeparated\n" + "\n".join(rows)
    resp = requests.post(CH_URL, data=data.encode())
    if resp.status_code == 200:
        log.info(f"Inserted {len(rows)} transactions into ClickHouse")
    else:
        log.error(f"ClickHouse error: {resp.text}")

def main():
    log.info("ClickHouse consumer started")
    batch = []
    last_flush = time.time()
    for message in consumer:
        try:
            batch.append(message.value)
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL:
                insert_batch(batch)
                batch = []
                last_flush = time.time()
        except Exception as e:
            log.error(f"Error: {e}")

if __name__ == "__main__":
    main()