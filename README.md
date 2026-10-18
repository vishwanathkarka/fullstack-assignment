# Transaction Webhook Service

Accepts transaction webhooks, returns `202` right away, and processes each
transaction in the background (30s simulated delay). Data is stored in MySQL.

Live: http://157.173.222.179:8090

## Endpoints

- `GET /` - health check
- `POST /v1/webhooks/transactions` - receive a webhook
- `GET /v1/transactions/{transaction_id}` - check status

## Run locally

```bash
docker compose up --build
```

API runs on http://localhost:8000. Without Docker:

```bash
pip install -r requirements.txt
export DATABASE_URL=mysql://root:root@127.0.0.1:3306/transactions
uvicorn app.main:app --reload
```

The table is created on startup.

## Test

```bash
curl -X POST http://localhost:8000/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

curl http://localhost:8000/v1/transactions/txn_abc123def456
```

Status is `PROCESSING` at first and `PROCESSED` after about 30 seconds.
Sending the same webhook again returns `202` but it is not processed twice.

There's also a script that sends duplicates, a batch of parallel requests, and
waits for processing:

```bash
python scripts/smoke_test.py http://localhost:8000
```

## Technical choices

- **FastAPI + aiomysql** - async fits well since the work is mostly waiting on the DB and the 30s delay.
- **The webhook only inserts a row and returns.** Processing happens in a background worker inside the same app, so the response stays fast.
- **Duplicates** are handled by making `transaction_id` the primary key. A second insert fails with a duplicate key error and is ignored.
- **The table is also the queue.** The worker claims rows with `SELECT ... FOR UPDATE SKIP LOCKED` and sets a `locked_until` time. If the app crashes mid-processing, the row is picked up again after restart, so nothing is lost. I didn't use Redis/Celery to keep it to one service.
- **Deployed** with Docker on a VPS using the MySQL server on that machine.
