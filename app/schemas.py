from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class TransactionWebhook(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=128)
    source_account: str = Field(min_length=1, max_length=128)
    destination_account: str = Field(min_length=1, max_length=128)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Za-z]{3}$")

    @field_validator("currency")
    @classmethod
    def upper_currency(cls, v: str) -> str:
        return v.upper()


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def transaction_out(row) -> dict:
    return {
        "transaction_id": row["transaction_id"],
        "source_account": row["source_account"],
        "destination_account": row["destination_account"],
        "amount": float(row["amount"]),
        "currency": row["currency"],
        "status": row["status"],
        "created_at": iso(row["created_at"]),
        "processed_at": iso(row["processed_at"]),
    }
