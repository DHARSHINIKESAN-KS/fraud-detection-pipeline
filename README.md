# Real-time Fraud Detection Pipeline

Production-grade fraud detection system that scores every payment transaction in real time using Kafka event streaming, a custom Python rules engine, and Redis decision cache — serving decisions via FastAPI in under 100ms.

---

## Overview

Payment fraud costs companies billions annually. This system replicates the core architecture used by fintech companies like Razorpay and PhonePe — a streaming pipeline that ingests payment events, scores them against fraud rules, and returns a BLOCK/REVIEW/ALLOW decision before the payment completes.

---

## Architecture
Payment Events

↓

Apache Kafka (txn-events topic)

↓

Python Rules Engine

├── Rule 1: Velocity check (>5 txns/5min = flagged)

├── Rule 2: Amount spike (>3x user average = flagged)

└── Rule 3: New merchant + high amount = flagged

↓

Fraud Score (0.0 → 1.0)

├── ≥ 0.7 → BLOCK

├── 0.4–0.7 → REVIEW

└── < 0.4 → ALLOW

↓

Redis Decision Cache (idempotent, 24hr TTL)

↓

FastAPI Decision Service

├── POST /score    → real-time decision

├── GET  /explain  → why was this flagged?

└── GET  /metrics  → Prometheus export

↓

ClickHouse (permanent analytics storage)

fraud-alerts Kafka topic → analyst review

txn-dead-letter topic → failed event retry

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Event Streaming | Apache Kafka | Decouples ingestion from scoring |
| Rules Engine | Python (custom) | Velocity, amount spike, merchant risk |
| Decision Cache | Redis | Sub-millisecond lookups, idempotency |
| API | FastAPI | REST decision service |
| Analytics | ClickHouse | Historical fraud pattern queries |
| Metrics | Prometheus | Latency and fraud rate monitoring |
| Infrastructure | Docker Compose | Local production-equivalent setup |

---

## Benchmarks

| Metric | Result |
|---|---|
| Throughput | 456 requests/min, 0% error rate |
| p50 latency | 23ms |
| p95 latency | 34ms |
| p99 latency | 41ms |
| Transactions stored | 9,000+ rows in ClickHouse |
| Duplicate decisions | 0 (exactly-once guaranteed) |


---

## Features

- Custom Python rules engine — velocity checks, amount spike detection, merchant risk scoring
- Idempotent decision API — same transaction always returns same decision under retry
- Explainability API — shows exactly which rules triggered and why
- Dead letter queue — failed transactions saved for retry, no data loss
- Prometheus metrics export — decisions/sec, p99 latency, fraud rate
- ClickHouse analytics — fraud rate by merchant, hourly patterns, score distribution

---

## Running Locally

```bash
# 1. Start Kafka + Redis
cd infra/kafka && docker-compose up -d

# 2. Start ClickHouse
cd infra/clickhouse && docker-compose up -d

# 3. Start transaction producer
cd services/producer && python producer.py

# 4. Start rules engine
cd services/rules-engine && python rules_engine.py

# 5. Start ClickHouse consumer
cd services/ch-consumer && python ch_consumer.py

# 6. Start decision API
cd services/api && uvicorn main:app --port 8002

# 7. Test the API
curl -X POST "http://localhost:8002/score" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"USR001","amount":150000,"is_new_merchant":true}'
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | /health | Service health check |
| POST | /score | Score transaction, return decision |
| GET | /explain/{txn_id} | Why was this transaction flagged? |
| GET | /metrics | Prometheus metrics scrape endpoint |

---

## Sample API Response

```json
POST /score
{
  "user_id": "USR001",
  "amount": 150000,
  "is_new_merchant": true
}

Response:
{
  "txn_id": "a3f2c1d0-...",
  "decision": "BLOCK",
  "risk_score": 0.7,
  "reasons": ["amount_spike", "new_merchant_high_amount"]
}
```

---

## Analytics Queries

```sql
-- Fraud rate by merchant category
SELECT merchant_category,
       countIf(decision='BLOCK') as blocked,
       count() as total
FROM fraud_detection.transactions
GROUP BY merchant_category ORDER BY blocked DESC

-- Hourly fraud patterns
SELECT toStartOfHour(timestamp) as hour,
       count() as txns,
       avg(risk_score) as avg_risk
FROM fraud_detection.transactions
GROUP BY hour ORDER BY hour DESC

-- Score distribution
SELECT decision, avg(risk_score), count()
FROM fraud_detection.transactions
GROUP BY decision
```

---

## Project Structure
fraud-detection-pipeline/

├── services/

│   ├── producer/        Payment event generator

│   ├── rules-engine/    Kafka consumer + fraud rules + scoring

│   ├── api/             FastAPI decision service

│   └── ch-consumer/     Kafka → ClickHouse ingestion

├── infra/

│   ├── kafka/           Kafka + Redis Docker setup

│   └── clickhouse/      ClickHouse schema + config

└── benchmarks/          Load test + idempotency verification