import json
import time
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis
from kafka import KafkaProducer

app = FastAPI(title="Fraud Detection Decision API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

r = redis.Redis(host='localhost', port=6380, decode_responses=True)

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

VELOCITY_WINDOW = 300
VELOCITY_THRESHOLD = 5
AMOUNT_SPIKE_MULTIPLIER = 3

class Transaction(BaseModel):
    user_id: str
    amount: float
    merchant_id: str = "MER000"
    merchant_category: str = "general"
    is_new_merchant: bool = False
    payment_method: str = "UPI"
    location: str = "Unknown"

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
    if risk_score >= 0.7:
        return "BLOCK"
    elif risk_score >= 0.4:
        return "REVIEW"
    return "ALLOW"

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/score")
def score_transaction(txn: Transaction):
    """Score a transaction and return fraud decision in real-time"""
    txn_id = str(uuid.uuid4())

    # Idempotency check (in case same request retried with explicit txn_id later)
    velocity_flag, velocity_count = check_velocity(txn.user_id)
    amount_flag, avg_amount = check_amount_spike(txn.user_id, txn.amount)
    merchant_flag = check_new_merchant_risk(txn.is_new_merchant, txn.amount)

    risk_score, reasons = compute_risk_score(velocity_flag, amount_flag, merchant_flag)
    decision = get_decision(risk_score)

    record = {
        "txn_id": txn_id,
        "user_id": txn.user_id,
        "amount": txn.amount,
        "risk_score": risk_score,
        "reasons": json.dumps(reasons),
        "decision": decision,
        "velocity_count": velocity_count,
        "user_avg_amount": round(avg_amount, 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    r.hset(f"decision:{txn_id}", mapping=record)
    r.expire(f"decision:{txn_id}", 86400)

    if decision != "ALLOW":
        producer.send('fraud-alerts', value=record)

    return {
        "txn_id": txn_id,
        "decision": decision,
        "risk_score": risk_score,
        "reasons": reasons
    }

@app.get("/explain/{txn_id}")
def explain_decision(txn_id: str):
    """Returns full explanation of why a transaction was scored this way"""
    data = r.hgetall(f"decision:{txn_id}")
    if not data:
        raise HTTPException(status_code=404, detail=f"No decision found for txn_id: {txn_id}")
    
    return {
        "txn_id": data['txn_id'],
        "user_id": data['user_id'],
        "amount": float(data['amount']),
        "decision": data['decision'],
        "risk_score": float(data['risk_score']),
        "reasons": json.loads(data['reasons']),
        "velocity_count": data.get('velocity_count', 'N/A'),
        "user_avg_amount": data.get('user_avg_amount', 'N/A'),
        "timestamp": data['timestamp']
    }