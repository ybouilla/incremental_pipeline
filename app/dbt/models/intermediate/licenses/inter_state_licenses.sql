--Build the full state-change history.
-- This gives one row per license per effective change date.
with initial_state as (

    select
        license_id,
        creation_date as date,
        renewable,
        price,
        type,
        -1 as id

    from {{ ref('stg_initial_licenses') }}

),

filtered_initial as (

    select *
    from initial_state i

    where not exists (

        select 1
        from {{ ref('stg_license_changes') }} c
        where c.license_id = i.license_id
          and c.date = i.date

    )

),

states as (

    select *
    from {{ ref('stg_license_changes') }}

    union all

    select *
    from filtered_initial

),

final as (

    select *
    from (

        select *,
            row_number() over (
                partition by license_id, date
                order by id desc
            ) rn
        from states

    ) s
    where rn = 1

)

select *
from final