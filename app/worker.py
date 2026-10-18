import asyncio
import logging

from app import db
from app.config import Settings

log = logging.getLogger("worker")


async def process_transaction(txn, delay: float) -> None:
    # Stand-in for the real work (fraud checks, ledger calls, etc).
    await asyncio.sleep(delay)


class TransactionWorker:
    def __init__(self, pool, settings: Settings):
        self.pool = pool
        self.settings = settings
        self._wakeup = asyncio.Event()
        self._in_flight: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None

    def start(self) -> None:
        self._loop_task = asyncio.create_task(self._run(), name="txn-worker")

    def notify(self) -> None:
        """Called by the webhook handler so new rows get picked up right away
        instead of waiting for the next poll."""
        self._wakeup.set()

    async def stop(self) -> None:
        if self._loop_task:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
        tasks = list(self._in_flight.values())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # release leases so a restart can pick these up straight away
        try:
            await db.release(self.pool, list(self._in_flight.keys()))
        except Exception:
            log.exception("could not release in-flight transactions")

    async def _run(self) -> None:
        while True:
            try:
                free = self.settings.worker_concurrency - len(self._in_flight)
                if free > 0:
                    rows = await db.claim_pending(self.pool, free, self.settings.lease_seconds)
                    for row in rows:
                        txn_id = row["transaction_id"]
                        task = asyncio.create_task(self._handle(row))
                        self._in_flight[txn_id] = task
                        task.add_done_callback(lambda _t, k=txn_id: self._in_flight.pop(k, None))
            except asyncio.CancelledError:
                raise
            except Exception:
                # try again on the next tick
                log.exception("worker poll failed")

            try:
                await asyncio.wait_for(self._wakeup.wait(), self.settings.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass
            self._wakeup.clear()

    async def _handle(self, row) -> None:
        txn_id = row["transaction_id"]
        log.info("processing %s (attempt %s)", txn_id, row["attempts"])
        try:
            await process_transaction(row, self.settings.processing_delay_seconds)
            await db.mark_processed(self.pool, txn_id)
            log.info("processed %s", txn_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            backoff = min(2 ** row["attempts"], 60)
            log.warning("processing %s failed: %s (retry in %ss)", txn_id, e, backoff)
            try:
                await db.mark_failed_attempt(
                    self.pool, txn_id, str(e), backoff, self.settings.max_attempts
                )
            except Exception:
                # lease will expire and it gets retried anyway
                log.exception("could not record failure for %s", txn_id)
