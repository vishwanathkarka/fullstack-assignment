import re
import warnings
from urllib.parse import unquote, urlparse

import aiomysql
import pymysql

DUPLICATE_KEY = 1062

# silence "already exists" notes from CREATE ... IF NOT EXISTS
warnings.filterwarnings("ignore", category=pymysql.Warning)

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      VARCHAR(128)   NOT NULL PRIMARY KEY,
    source_account      VARCHAR(128)   NOT NULL,
    destination_account VARCHAR(128)   NOT NULL,
    amount              DECIMAL(18, 2) NOT NULL,
    currency            CHAR(3)        NOT NULL,
    status              VARCHAR(16)    NOT NULL DEFAULT 'PROCESSING',
    attempts            INT            NOT NULL DEFAULT 0,
    last_error          VARCHAR(500)   NULL,
    locked_until        DATETIME(6)    NULL,
    created_at          DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at        DATETIME(6)    NULL,
    KEY idx_status_created (status, created_at)
) ENGINE=InnoDB
"""


def parse_url(url: str) -> dict:
    u = urlparse(url)
    db = u.path.lstrip("/") or "transactions"
    if not re.fullmatch(r"\w+", db):
        raise ValueError(f"bad database name: {db!r}")
    return {
        "host": u.hostname or "127.0.0.1",
        "port": u.port or 3306,
        "user": unquote(u.username or "root"),
        "password": unquote(u.password or ""),
        "db": db,
    }


async def create_pool(url: str) -> aiomysql.Pool:
    cfg = parse_url(url)
    # READ COMMITTED avoids gap locks blocking webhook inserts while the worker claims rows
    session_setup = "SET time_zone = '+00:00', SESSION transaction_isolation = 'READ-COMMITTED'"

    conn = await aiomysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"]
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{cfg['db']}`")
    finally:
        conn.close()

    pool = await aiomysql.create_pool(
        **cfg,
        minsize=10,
        maxsize=20,
        autocommit=True,
        init_command=session_setup,
        cursorclass=aiomysql.DictCursor,
        pool_recycle=3600,
    )
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(SCHEMA)
    return pool


async def close_pool(pool: aiomysql.Pool) -> None:
    pool.close()
    await pool.wait_closed()


async def execute(pool: aiomysql.Pool, sql: str, args=None) -> int:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(sql, args)
        return cur.rowcount


async def insert_transaction(pool: aiomysql.Pool, txn) -> bool:
    """Returns True if this is the first time we've seen the transaction."""
    try:
        await execute(
            pool,
            """
            INSERT INTO transactions
                (transaction_id, source_account, destination_account, amount, currency)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                txn.transaction_id,
                txn.source_account,
                txn.destination_account,
                txn.amount,
                txn.currency,
            ),
        )
        return True
    except pymysql.err.IntegrityError as e:
        # primary key already exists -> duplicate webhook
        if e.args[0] == DUPLICATE_KEY:
            return False
        raise


async def get_transaction(pool: aiomysql.Pool, transaction_id: str):
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT * FROM transactions WHERE transaction_id = %s", (transaction_id,))
        return await cur.fetchone()


async def claim_pending(pool: aiomysql.Pool, limit: int, lease_seconds: int) -> list[dict]:
    # if a worker dies mid-processing, the row is picked up again once locked_until passes
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT * FROM transactions
                    WHERE status = 'PROCESSING'
                      AND (locked_until IS NULL OR locked_until < NOW(6))
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (limit,),
                )
                rows = list(await cur.fetchall())
                if rows:
                    ids = [r["transaction_id"] for r in rows]
                    placeholders = ", ".join(["%s"] * len(ids))
                    await cur.execute(
                        f"""
                        UPDATE transactions
                        SET locked_until = NOW(6) + INTERVAL %s SECOND,
                            attempts = attempts + 1
                        WHERE transaction_id IN ({placeholders})
                        """,
                        (lease_seconds, *ids),
                    )
            await conn.commit()
        except BaseException:
            # rollback can hang on a half-finished query, closing makes mysql roll back
            conn.close()
            raise

    for r in rows:
        r["attempts"] += 1
    return rows


async def mark_processed(pool: aiomysql.Pool, transaction_id: str) -> None:
    await execute(
        pool,
        """
        UPDATE transactions
        SET status = 'PROCESSED', processed_at = NOW(6), locked_until = NULL, last_error = NULL
        WHERE transaction_id = %s AND status = 'PROCESSING'
        """,
        (transaction_id,),
    )


async def mark_failed_attempt(
    pool: aiomysql.Pool, transaction_id: str, error: str, retry_in: float, max_attempts: int
) -> None:
    await execute(
        pool,
        """
        UPDATE transactions
        SET last_error = %s,
            locked_until = NOW(6) + INTERVAL %s SECOND,
            status = CASE WHEN attempts >= %s THEN 'FAILED' ELSE status END
        WHERE transaction_id = %s AND status = 'PROCESSING'
        """,
        (error[:500], int(retry_in), max_attempts, transaction_id),
    )


async def release(pool: aiomysql.Pool, transaction_ids: list[str]) -> None:
    if not transaction_ids:
        return
    placeholders = ", ".join(["%s"] * len(transaction_ids))
    await execute(
        pool,
        f"""
        UPDATE transactions SET locked_until = NULL, attempts = GREATEST(attempts - 1, 0)
        WHERE transaction_id IN ({placeholders}) AND status = 'PROCESSING'
        """,
        transaction_ids,
    )
