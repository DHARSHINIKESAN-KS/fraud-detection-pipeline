# Runbook

## Start services
```bash
cd infra/kafka && docker-compose up -d
cd infra/clickhouse && docker-compose up -d
python services/producer/producer.py
python services/rules-engine/rules_engine.py
uvicorn services/api/main:app --port 8002
```

## Check fraud decisions
```bash
curl http://localhost:8002/health
```
