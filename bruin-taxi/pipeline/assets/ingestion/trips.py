"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: Vendor identifier from the TLC trip parquet source.
  - name: tpep_pickup_datetime
    type: timestamp
    description: Yellow taxi pickup timestamp from the raw source.
  - name: tpep_dropoff_datetime
    type: timestamp
    description: Yellow taxi dropoff timestamp from the raw source.
  - name: lpep_pickup_datetime
    type: timestamp
    description: Green taxi pickup timestamp from the raw source.
  - name: lpep_dropoff_datetime
    type: timestamp
    description: Green taxi dropoff timestamp from the raw source.
  - name: passenger_count
    type: numeric
    description: Passenger count reported in the raw trip file.
  - name: trip_distance
    type: numeric
    description: Trip distance reported in the raw trip file.
  - name: RatecodeID
    type: numeric
    description: Rate code identifier from the raw trip file.
  - name: store_and_fwd_flag
    type: string
    description: Raw store-and-forward flag from the trip file.
  - name: PULocationID
    type: integer
    description: Pickup location identifier from the raw trip file.
  - name: DOLocationID
    type: integer
    description: Dropoff location identifier from the raw trip file.
  - name: payment_type
    type: integer
    description: Raw payment type identifier from the trip file.
  - name: fare_amount
    type: numeric
    description: Fare amount from the raw trip file.
  - name: extra
    type: numeric
    description: Extra charges from the raw trip file.
  - name: mta_tax
    type: numeric
    description: MTA tax from the raw trip file.
  - name: tip_amount
    type: numeric
    description: Tip amount from the raw trip file.
  - name: tolls_amount
    type: numeric
    description: Tolls amount from the raw trip file.
  - name: improvement_surcharge
    type: numeric
    description: Improvement surcharge from the raw trip file.
  - name: total_amount
    type: numeric
    description: Total amount from the raw trip file.
  - name: congestion_surcharge
    type: numeric
    description: Congestion surcharge from the raw trip file.
  - name: airport_fee
    type: numeric
    description: Airport fee from the raw trip file when present.
  - name: trip_type
    type: numeric
    description: Green taxi trip type from the raw trip file when present.
  - name: ehail_fee
    type: numeric
    description: E-hail fee from the raw trip file when present.
  - name: taxi_type
    type: string
    description: Taxi type used to build the source file URL.
  - name: source_url
    type: string
    description: Source parquet URL fetched for this row batch.
  - name: source_month
    type: string
    description: Month portion of the source file name in YYYY-MM format.
  - name: extracted_at
    type: timestamp
    description: UTC timestamp when the parquet file was fetched.

@bruin"""

import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
EXPECTED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "trip_type",
    "ehail_fee",
    "taxi_type",
    "source_url",
    "source_month",
    "extracted_at",
]


def _load_taxi_types() -> list[str]:
    raw_vars = os.environ.get("BRUIN_VARS", "{}")
    parsed_vars = json.loads(raw_vars)
    taxi_types = parsed_vars.get("taxi_types", ["green"])

    if not isinstance(taxi_types, list) or not taxi_types:
        raise ValueError("BRUIN_VARS.taxi_types must be a non-empty array of strings.")

    return [str(taxi_type) for taxi_type in taxi_types]


def _month_starts(start_date: str, end_date: str) -> list[pd.Timestamp]:
    start = pd.Timestamp(start_date).replace(day=1)
    end = pd.Timestamp(end_date)
    months: list[pd.Timestamp] = []
    current = start

    while current < end:
        months.append(current)
        current = current + relativedelta(months=1)

    return months


def _fetch_month(taxi_type: str, month_start: pd.Timestamp) -> pd.DataFrame | None:
    source_month = month_start.strftime("%Y-%m")
    source_url = f"{BASE_URL}/{taxi_type}_tripdata_{source_month}.parquet"

    print(f"Downloading: {source_url}")
    response = requests.get(source_url, timeout=120)
    if response.status_code == 404:
        return None

    response.raise_for_status()

    dataframe = pd.read_parquet(io.BytesIO(response.content))
    dataframe["taxi_type"] = taxi_type
    dataframe["source_url"] = source_url
    dataframe["source_month"] = source_month
    dataframe["extracted_at"] = datetime.now(timezone.utc)
    for column in EXPECTED_COLUMNS:
        if column not in dataframe.columns:
            dataframe[column] = pd.NA

    dataframe = dataframe[EXPECTED_COLUMNS]
    return dataframe


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    frames: list[pd.DataFrame] = []
    for taxi_type in _load_taxi_types():
        for month_start in _month_starts(start_date, end_date):
            frame = _fetch_month(taxi_type, month_start)
            if frame is not None:
                frames.append(frame)

    if not frames:
        return pd.DataFrame(
            columns=["taxi_type", "source_url", "source_month", "extracted_at"]
        )

    return pd.concat(frames, ignore_index=True, sort=False)
