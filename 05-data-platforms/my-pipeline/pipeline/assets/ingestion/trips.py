
"""@bruin

name: ingestion.trips
type: python
connection: duckdb-default
image: python:3.11

materialization:
  type: table
  strategy: append

columns:
  - name: trip_hash
    type: string
    description: surrogate hash for deduplication
    primary_key: true
  - name: taxi_type
    type: string
    description: taxi color/type
  - name: pickup_datetime
    type: timestamp
    description: pickup timestamp from source
  - name: dropoff_datetime
    type: timestamp
    description: dropoff timestamp from source
  - name: passenger_count
    type: integer
    description: passenger count
  - name: payment_type
    type: integer
    description: numeric payment code from source
  - name: fare_amount
    type: double
    description: fare amount from source
  - name: extracted_at
    type: timestamp
    description: extraction timestamp

@bruin"""

import os
import json
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _months_between(start_date: date, end_date: date):
    cur = start_date.replace(day=1)
    while cur < end_date:
        yield cur.year, cur.month
        cur = (cur + relativedelta(months=1))


def materialize():
    """Fetch parquet files for the requested month range and taxi types.

    Expects BRUIN_START_DATE / BRUIN_END_DATE env vars and BRUIN_VARS JSON for pipeline variables.
    Returns a pandas.DataFrame (Bruin will load it into the destination).
    """
    start_date_s = os.environ.get("BRUIN_START_DATE")
    end_date_s = os.environ.get("BRUIN_END_DATE")

    if not start_date_s or not end_date_s:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set by Bruin runtime")

    start_date = datetime.fromisoformat(start_date_s).date()
    end_date = datetime.fromisoformat(end_date_s).date()

    # pipeline vars
    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    try:
        vars_json = json.loads(bruin_vars)
    except Exception:
        vars_json = {}

    taxi_types = vars_json.get("taxi_types", ["yellow"]) or ["yellow"]

    dfs = []
    for year, month in _months_between(start_date, end_date):
        ym = f"{year}-{month:02d}"
        for taxi in taxi_types:
            filename = f"{taxi}_tripdata_{ym}.parquet"
            url = BASE_URL + filename
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                bio = io.BytesIO(resp.content)
                df = pd.read_parquet(bio, engine="pyarrow")
                df["taxi_type"] = taxi
                dfs.append(df)
            except requests.HTTPError:
                # Missing file for that month/taxi is not fatal; skip
                continue

    if not dfs:
        # return empty frame with expected columns
        return pd.DataFrame(columns=[
            "trip_hash",
            "taxi_type",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "payment_type",
            "fare_amount",
            "extracted_at",
        ])

    out = pd.concat(dfs, ignore_index=True, sort=False)
    out["extracted_at"] = datetime.utcnow()

    # best-effort create a surrogate hash for deduplication
    # use row-wise concatenation of a few stable columns if present
    def _make_hash(row):
        parts = []
        for c in ["pickup_datetime", "dropoff_datetime", "passenger_count", "fare_amount", "taxi_type"]:
            parts.append(str(row.get(c, "")))
        return "|".join(parts)

    out["trip_hash"] = out.apply(lambda r: _make_hash(r), axis=1)

    # keep raw columns as-is; downstream staging handles dedup/normalization
    return out


