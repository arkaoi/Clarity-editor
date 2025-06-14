#!/usr/bin/env python3
import requests
import threading
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "http://localhost:8080/database"
NUM_THREADS = 8
OPS_PER_THREAD = 10000
MAX_CONNECT_RETRIES = 5

retry_strategy = Retry(
    total=3,
    connect=MAX_CONNECT_RETRIES,
    read=2,
    backoff_factor=0.1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "PUT", "DELETE"],
)
adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=NUM_THREADS,
    pool_maxsize=NUM_THREADS,
)

def random_key(tid: int, i: int) -> str:
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"t{tid}_{i}_{suffix}"

def random_value():
    if random.random() < 0.5:
        return {
            "num": random.randint(0, 1000),
            "text": ''.join(random.choices(string.ascii_letters, k=12))
        }
    else:
        return {
            "nested": {
                "float": random.random(),
                "list": [random.randint(0, 50) for _ in range(5)],
                "map": {c: random.randint(0, 9) for c in random.sample(string.ascii_lowercase, 3)}
            }
        }

def do_request(session, method, url, **kwargs):
    for attempt in range(MAX_CONNECT_RETRIES):
        try:
            resp = session.request(method, url, timeout=2, **kwargs)
            return resp
        except requests.exceptions.ConnectionError as e:
            time.sleep(0.05)
    return session.request(method, url, timeout=2, **kwargs)

def worker(tid: int) -> int:
    session = requests.Session()
    session.mount("http://", adapter)
    success = 0

    for i in range(OPS_PER_THREAD):
        key = random_key(tid, i)
        value = random_value()
        url = f"{BASE_URL}/{key}"

        r = do_request(session, "PUT", url, json={"value": value})
        if r.status_code != 200:
            print(f"[T{tid}] PUT error {r.status_code} on {key}")
            continue

        r = do_request(session, "GET", url)
        if r.status_code != 200:
            print(f"[T{tid}] GET error {r.status_code} on {key}")
            continue
        got = r.json().get("value")
        if got != value:
            print(f"[T{tid}] MISMATCH {key}: sent={value} got={got}")
            continue

        r = do_request(session, "DELETE", url)
        if r.status_code != 200:
            print(f"[T{tid}] DELETE error {r.status_code} on {key}")
            continue

        r = do_request(session, "GET", url)
        if r.status_code != 404:
            print(f"[T{tid}] AFTER-DELETE GET returned {r.status_code} on {key}")
            continue

        success += 1

    session.close()
    return success

def main():
    print(f"Starting test: {NUM_THREADS} threads × {OPS_PER_THREAD} ops")
    start = time.time()
    total_success = 0

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as ex:
        futures = [ex.submit(worker, tid) for tid in range(NUM_THREADS)]
        for f in as_completed(futures):
            total_success += f.result()

    duration = time.time() - start
    expected = NUM_THREADS * OPS_PER_THREAD
    print(f"Done in {duration:.2f}s: {total_success}/{expected} successful operations")

    if total_success == expected:
        print("ALL TESTS PASSED")
        exit(0)
    else:
        print("SOME TESTS FAILED")
        exit(1)

if __name__ == "__main__":
    main()