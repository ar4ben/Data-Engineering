/* @bruin

name: staging.trips
type: duckdb.sql
depends:
   - ingestion.trips
   - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null



# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1

@bruin */

WITH trips_in_window AS (
    SELECT
        vendor_id,
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id AS pickup_location_id,
        do_location_id AS dropoff_location_id,
        trips.payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        NULL AS airport_fee,
        trip_type,
        ehail_fee,
        taxi_type,
        source_url,
        source_month,
        extracted_at
    FROM ingestion.trips AS trips
    WHERE lpep_pickup_datetime >= '{{ start_datetime }}'
      AND lpep_pickup_datetime < '{{ end_datetime }}'
),
deduplicated_trips AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                ratecode_id,
                store_and_fwd_flag,
                pickup_location_id,
                dropoff_location_id,
                payment_type,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount,
                congestion_surcharge,
                airport_fee,
                trip_type,
                ehail_fee,
                taxi_type,
                source_url,
                source_month
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM trips_in_window
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND dropoff_datetime >= pickup_datetime
      AND COALESCE(passenger_count, 0) >= 0
      AND COALESCE(trip_distance, 0) >= 0
      AND COALESCE(fare_amount, 0) >= 0
      AND COALESCE(total_amount, 0) >= 0
)
SELECT
    trips.vendor_id,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.passenger_count,
    trips.trip_distance,
    trips.ratecode_id,
    trips.store_and_fwd_flag,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.payment_type,
    payment_lookup.payment_type_name,
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.trip_type,
    trips.ehail_fee,
    trips.taxi_type,
    trips.source_url,
    trips.source_month,
    trips.extracted_at
FROM deduplicated_trips AS trips
LEFT JOIN ingestion.payment_lookup AS payment_lookup
    ON trips.payment_type = payment_lookup.payment_type_id
WHERE trips.row_num = 1
