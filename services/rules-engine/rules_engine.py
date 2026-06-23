import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'txn-events',
    bootstrap_servers='localhost:9093',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='rules-engine-v2',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

r = redis.Redis(host='localhost', port=6380, decode_responses=True)

VELOCITY_WINDOW = 300
VELOCITY_THRESHOLD = 5
AMOUNT_SPIKE_MULTIPLIER = 3

def check_velocity(user_id):
    key = f"velocity:{user_id}"
    now = time.time()
    r.zadd(key, {str(now): now})
    r.zremrangebyscore(key, 0, now - VELOCITY_WINDOW)
    r.expire(key, VELOCITY_WINDOW)
    count = r.zcard(key)
    return count > VELOCITY_THRESHOLD, count

def check_amount_spike(user_id, amount):
    avg_key = f"avg_amount:{user_id}"
    count_key = f"avg_count:{user_id}"
    current_avg = r.get(avg_key)
    current_count = r.get(count_key)
    if current_avg is None:
        r.set(avg_key, amount, ex=604800)
        r.set(count_key, 1, ex=604800)
        return False, amount
    current_avg = float(current_avg)
    current_count = int(current_count)
    flagged = amount > (current_avg * AMOUNT_SPIKE_MULTIPLIER)
    new_count = current_count + 1
    new_avg = ((current_avg * current_count) + amount) / new_count
    r.set(avg_key, new_avg, ex=604800)
    r.set(count_key, new_count, ex=604800)
    return flagged, current_avg

def check_new_merchant_risk(is_new_merchant, amount):
    return is_new_merchant and amount > 10000

def compute_risk_score(velocity_flag, amount_flag, merchant_flag):
    score = 0.0
    reasons = []
    if velocity_flag:
        score += 0.4
        reasons.append("high_velocity")
    if amount_flag:
        score += 0.4
        reasons.append("amount_spike")
    if merchant_flag:
        score += 0.3
        reasons.append("new_merchant_high_amount")
    return min(score, 1.0), reasons

def get_decision(risk_score):
    """Convert risk score to human decision"""
    if risk_score >= 0.7:
        return "BLOCK"
    elif risk_score >= 0.4:
        return "REVIEW"
    else:
        return "ALLOW"

def is_duplicate(txn_id):
    """Idempotency check — has this transaction been processed before?"""
    return r.exists(f"decision:{txn_id}")

def store_decision(txn_id, user_id, amount, risk_score, reasons, decision):
    """Store final decision in Redis permanently"""
    record = {
        "txn_id": txn_id,
        "user_id": user_id,
        "amount": amount,
        "risk_score": risk_score,
        "reasons": json.dumps(reasons),
        "decision": decision,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    r.hset(f"decision:{txn_id}", mapping=record)
    r.expire(f"decision:{txn_id}", 86400)  # 24 hours
    return record

def process_transaction(txn):
    txn_id = txn['txn_id']
    user_id = txn['user_id']
    amount = txn['amount']
    is_new_merchant = txn.get('is_new_merchant', False)

    # IDEMPOTENCY CHECK — most important part
    if is_duplicate(txn_id):
        existing = r.hgetall(f"decision:{txn_id}")
        log.info(f"DUPLICATE | txn={txn_id[:8]} | returning cached decision={existing['decision']}")
        return

    # Run rules
    velocity_flag, velocity_count = check_velocity(user_id)
    amount_flag, avg_amount = check_amount_spike(user_id, amount)
    merchant_flag = check_new_merchant_risk(is_new_merchant, amount)

    risk_score, reasons = compute_risk_score(velocity_flag, amount_flag, merchant_flag)
    decision = get_decision(risk_score)

    record = store_decision(txn_id, user_id, amount, risk_score, reasons, decision)

    # Publish to fraud-alerts if not ALLOW
    if decision != "ALLOW":
        producer.send('fraud-alerts', value=record)

    if decision == "BLOCK":
        log.warning(f"BLOCK | txn={txn_id[:8]} | user={user_id} | score={risk_score} | reasons={reasons}")
    elif decision == "REVIEW":
        log.warning(f"REVIEW | txn={txn_id[:8]} | user={user_id} | score={risk_score} | reasons={reasons}")
    else:
        log.info(f"ALLOW | txn={txn_id[:8]} | user={user_id} | amount=₹{amount}")

def send_to_dlq(raw_message, error):
    """Send failed transaction to dead letter queue for later review"""
    dlq_record = {
        "original_message": raw_message,
        "error": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat()
    }
    producer.send('txn-dead-letter', value=dlq_record)
    log.error(f"Sent to DLQ: {error}")

def main():
    log.info("Rules engine v2 started — with idempotency + decision thresholds + DLQ")
    for message in consumer:
        try:
            process_transaction(message.value)
        except Exception as e:
            send_to_dlq(message.value, e)

if __name__ == "__main__":
    main()