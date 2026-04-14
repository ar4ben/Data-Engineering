-- incremental: furst run - create table, all following - update the table
-- unique_key: which key to use for update (mandatory for merge)
-- strategy merge: create rows if new, update if exist. Possible alternative: append (only create new as insert into target select ... where trips.pickup_datetime > (select max(pickup_datetime) from target))
-- append_new_columns - add new columns if not existed
{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'  )
}}

-- it will be converted into:
-- MERGE target t
-- USING source s
-- ON t.trip_id = s.trip_id
-- WHEN MATCHED THEN UPDATE
-- WHEN NOT MATCHED THEN INSERT

-- Fact table containing all taxi trips enriched with zone information
-- This is a classic star schema design: fact table (trips) joined to dimension table (zones)
-- Materialized incrementally to handle large datasets efficiently

select
    -- Trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    {{ get_trip_duration_minutes('trips.pickup_datetime', 'trips.dropoff_datetime') }} as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from {{ ref('int_trips') }} as trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
left join {{ ref('dim_zones') }} as pz
    on trips.pickup_location_id = pz.location_id
left join {{ ref('dim_zones') }} as dz
    on trips.dropoff_location_id = dz.location_id

-- if the table already exists ant this is not the first run:
{% if is_incremental() %}
  -- Only process new trips that don't exist in the table, based on pickup datetime
  where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }}) -- 'this' is the existed table from the first run.
{% endif %}