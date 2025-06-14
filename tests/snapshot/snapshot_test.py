#!/usr/bin/env python3
import requests
import csv
import json
import os
import tempfile
from pathlib import Path


BASE_URL = "http://localhost:8080"
SNAPSHOT_ENDPOINT = f"{BASE_URL}/snapshot"
DB_ENDPOINT = f"{BASE_URL}/database"
PROJECT_ROOT = Path(__file__).parent.resolve()
CSV_PATH = PROJECT_ROOT / "snapshot.csv"
NUM_ENTRIES = 500


def generate_value(i):
    return {
        "id": i,
        "name": f"item_{i}",
        "nested": {
            "flag": (i % 2 == 0),
            "values": [i, i*2, i*3],
        }
    }


def load_data():
    for i in range(NUM_ENTRIES):
        key = f"test_{i}"
        value = generate_value(i)
        r = requests.put(f"{DB_ENDPOINT}/{key}", json={"value": value})
        if r.status_code != 200:
            raise RuntimeError(f"PUT failed for {key}: {r.status_code}")


def fetch_snapshot():
    r = requests.get(SNAPSHOT_ENDPOINT)
    if r.status_code != 200:
        raise RuntimeError(f"Snapshot failed: {r.status_code}")
    csv_text = r.json()["snapshot_csv"]
    with open(CSV_PATH, "w", encoding="utf-8") as f:
        f.write(csv_text)

def verify_snapshot():
    found = {}
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = row["key"]
            v = json.loads(row["value"])
            found[k] = v

    for i in range(NUM_ENTRIES):
        key = f"test_{i}"
        if key not in found:
            raise AssertionError(f"Key missing in snapshot: {key}")
        expected = generate_value(i)
        actual = found[key]
        if actual != expected:
            raise AssertionError(f"Value mismatch for {key}: expected {expected}, got {actual}")

    print(f"OK: all {NUM_ENTRIES} entries are present and correct in {CSV_PATH}")

if __name__ == "__main__":
    print("Loading data...")
    load_data()
    print("Fetching snapshot...")
    fetch_snapshot()
    print("Verifying snapshot...")
    verify_snapshot()