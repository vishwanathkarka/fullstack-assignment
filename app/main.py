import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app import db
from app.config import Settings
from app.schemas import TransactionWebhook, iso, transaction_out
from app.worker import TransactionWorker

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    pool = await db.create_pool(settings.database_url)
    worker = TransactionWorker(pool, settings)
    worker.start()
    app.state.pool = pool
    app.state.worker = worker
    yield
    await worker.stop()
    await db.close_pool(pool)


app = FastAPI(title="Transaction Webhook Service", lifespan=lifespan)


@app.get("/")
async def health():
    return {"status": "HEALTHY", "current_time": iso(datetime.now(timezone.utc))}


@app.post("/v1/webhooks/transactions", status_code=202)
async def receive_webhook(payload: TransactionWebhook, request: Request):
    created = await db.insert_transaction(request.app.state.pool, payload)
    if created:
        request.app.state.worker.notify()
    else:
        log.info("duplicate webhook for %s, ignoring", payload.transaction_id)
    return {
        "transaction_id": payload.transaction_id,
        "status": "accepted" if created else "duplicate",
    }


@app.get("/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str, request: Request):
    row = await db.get_transaction(request.app.state.pool, transaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return [transaction_out(row)]


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse(status_code=500, content={"detail": "internal error"})
