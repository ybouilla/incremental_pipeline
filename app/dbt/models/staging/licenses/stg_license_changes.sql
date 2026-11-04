--Deduplicate change events.
{{ config(materialized='view') }}
with source as (

    select * from license_changes 

),

dedup as (

    select *
    from (

        select *,
            row_number() over (
                partition by license_id, cast(date as date)
                order by id desc
            ) as rn
        from source

    ) s
    where rn = 1

)

select
    license_id,
    cast(date as date) as date,
    renewable,
    price,
    type,
    id
from dedup