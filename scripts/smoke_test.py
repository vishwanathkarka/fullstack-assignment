"""Quick end-to-end check against a running instance.

    python scripts/smoke_test.py http://localhost:8000
    python scripts/smoke_test.py https://your-app.onrender.com --no-wait

Only uses the standard library so it can run anywhere.
"""
import argparse
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib import request
from urllib.error import HTTPError


def call(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    try:
        with request.urlopen(req, timeout=10) as resp:
            status, raw = resp.status, resp.read()
    except HTTPError as e:
        status, raw = e.code, e.read()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return status, json.loads(raw or "null"), elapsed_ms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url")
    parser.add_argument("--duplicates", type=int, default=5)
    parser.add_argument("--load", type=int, default=30, help="extra unique webhooks to fire in parallel")
    parser.add_argument("--no-wait", action="store_true", help="don't wait for processing to finish")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    status, body, ms = call("GET", base + "/")
    print(f"health: {status} {body} ({ms:.0f}ms)")

    txn_id = f"txn_{uuid.uuid4().hex[:12]}"
    payload = {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }

    print(f"\nsending {txn_id} {args.duplicates} times")
    for _ in range(args.duplicates):
        status, body, ms = call("POST", base + "/v1/webhooks/transactions", payload)
        print(f"  {status} {body['status'] if body else ''} ({ms:.0f}ms)")

    if args.load:
        print(f"\nfiring {args.load} unique webhooks in parallel")

        def one(i):
            p = dict(payload, transaction_id=f"{txn_id}_load_{i}")
            return call("POST", base + "/v1/webhooks/transactions", p)

        with ThreadPoolExecutor(max_workers=10) as ex:
            results = list(ex.map(one, range(args.load)))
        times = sorted(r[2] for r in results)
        codes = {r[0] for r in results}
        print(f"  status codes: {codes}")
        print(f"  latency p50={times[len(times) // 2]:.0f}ms max={times[-1]:.0f}ms")

    status, body, _ = call("GET", f"{base}/v1/transactions/{txn_id}")
    print(f"\nright after: {body}")
    if args.no_wait:
        return

    print("waiting for processing (~30s)...")
    started = time.time()
    while time.time() - started < 120:
        status, body, _ = call("GET", f"{base}/v1/transactions/{txn_id}")
        if body and body[0]["status"] != "PROCESSING":
            break
        time.sleep(2)
    print(f"after {time.time() - started:.0f}s: {body}")


if __name__ == "__main__":
    main()
