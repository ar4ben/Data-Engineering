with source as (
    select * from {{ source('staging', 'fhv_tripdata') }}
)


select
        -- identifiers
        cast(dispatching_base_num  as string) as dis_license_num,
        cast(Affiliated_base_number as string) as aff_license_num,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

         -- trip info
        cast(SR_Flag as integer) as shared_ride

        from source 
        where dis_license_num is not null

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
    and (pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01')
{% endif %}