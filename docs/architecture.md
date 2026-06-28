# System Architecture

## Data Flow

```
┌─────────────────────────────────────┐
│   Payment Event Producer            │
│   (simulates Razorpay/PhonePe txns) │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Apache Kafka                      │
│   txn-events / fraud-alerts /       │
│   txn-dead-letter                   │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Python Rules Engine               │
│   ├── Velocity check (5min window)  │
│   ├── Amount spike (3x avg)         │
│   └── New merchant risk             │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   Redis Decision Cache              │
│   Idempotent storage, 24hr TTL      │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   FastAPI Decision Service          │
│   POST /score  GET /explain         │
│   GET /metrics (Prometheus)         │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│   ClickHouse                        │
│   Historical analytics storage      │
└─────────────────────────────────────┘
```