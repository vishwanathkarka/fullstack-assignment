import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    processing_delay_seconds: float
    poll_interval_seconds: float
    lease_seconds: int
    max_attempts: int
    worker_concurrency: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv(
                "DATABASE_URL", "mysql://root:root@127.0.0.1:3306/transactions"
            ),
            processing_delay_seconds=float(os.getenv("PROCESSING_DELAY_SECONDS", "30")),
            poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "2")),
            # must be longer than the processing delay
            lease_seconds=int(os.getenv("LEASE_SECONDS", "120")),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", "5")),
            worker_concurrency=int(os.getenv("WORKER_CONCURRENCY", "100")),
        )
