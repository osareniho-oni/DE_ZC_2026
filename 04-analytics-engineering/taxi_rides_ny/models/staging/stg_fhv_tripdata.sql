with source as (

    select * from {{ source('wk1_tf_dataset', 'fhv_tripdata') }}

),

renamed as (

    select
        unique_row_id,
        filename,
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed