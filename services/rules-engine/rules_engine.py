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
    group_id='rules-engine',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

r = redis.Redis(host='localhost', port=6380, decode_responses=True)

VELOCITY_WINDOW = 300       # 5 minutes in seconds
VELOCITY_THRESHOLD = 5      # max 5 txns per window
AMOUNT_SPIKE_MULTIPLIER = 3 # flag if 3x average

def check_velocity(user_id):
    """Rule 1: Count transactions by this user in last 5 minutes"""
    key = f"velocity:{user_id}"
    now = time.time()
    
    # Add current timestamp to a sorted set
    r.zadd(key, {str(now): now})
    # Remove entries older than window
    r.zremrangebyscore(key, 0, now - VELOCITY_WINDOW)
    r.expire(key, VELOCITY_WINDOW)
    
    count = r.zcard(key)
    flagged = count > VELOCITY_THRESHOLD
    return flagged, count

def check_amount_spike(user_id, amount):
    """Rule 2: Compare against user's running average (7-day proxy using Redis)"""
    avg_key = f"avg_amount:{user_id}"
    count_key = f"avg_count:{user_id}"
    
    current_avg = r.get(avg_key)
    current_count = r.get(count_key)
    
    if current_avg is None:
        # First transaction for this user — no comparison possible
        r.set(avg_key, amount, ex=604800)  # 7 days TTL
        r.set(count_key, 1, ex=604800)
        return False, amount
    
    current_avg = float(current_avg)
    current_count = int(current_count)
    
    flagged = amount > (current_avg * AMOUNT_SPIKE_MULTIPLIER)
    
    # Update running average
    new_count = current_count + 1
    new_avg = ((current_avg * current_count) + amount) / new_count
    r.set(avg_key, new_avg, ex=604800)
    r.set(count_key, new_count, ex=604800)
    
    return flagged, current_avg

def check_new_merchant_risk(is_new_merchant, amount):
    """Rule 3: New/unknown merchant + high amount = risk"""
    HIGH_AMOUNT_THRESHOLD = 10000
    flagged = is_new_merchant and amount > HIGH_AMOUNT_THRESHOLD
    return flagged

def compute_risk_score(velocity_flag, amount_flag, merchant_flag):
    """Combine rule outputs into a 0.0-1.0 risk score"""
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
    
    score = min(score, 1.0)
    return score, reasons

def process_transaction(txn):
    user_id = txn['user_id']
    amount = txn['amount']
    is_new_merchant = txn.get('is_new_merchant', False)
    
    velocity_flag, velocity_count = check_velocity(user_id)
    amount_flag, avg_amount = check_amount_spike(user_id, amount)
    merchant_flag = check_new_merchant_risk(is_new_merchant, amount)
    
    risk_score, reasons = compute_risk_score(velocity_flag, amount_flag, merchant_flag)
    
    result = {
        "txn_id": txn['txn_id'],
        "user_id": user_id,
        "amount": amount,
        "risk_score": risk_score,
        "reasons": reasons,
        "velocity_count": velocity_count,
        "user_avg_amount": round(avg_amount, 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    # Store result in Redis for quick lookup
    r.set(f"risk:{txn['txn_id']}", json.dumps(result), ex=86400)  # 1 day TTL
    
    if risk_score > 0:
        producer.send('fraud-alerts', value=result)
        log.warning(f"RISK SCORE={risk_score} | txn={txn['txn_id'][:8]} | "
                    f"user={user_id} | reasons={reasons}")
    else:
        log.info(f"OK | txn={txn['txn_id'][:8]} | user={user_id} | amount=₹{amount}")

def main():
    log.info("Rules engine started — consuming txn-events")
    for message in consumer:
        try:
            process_transaction(message.value)
        except Exception as e:
            log.error(f"Error processing transaction: {e}")

if __name__ == "__main__":
    main()