--Add missing dates/types and calculate deltas.

with types as (

    select distinct type
    from {{ ref('inter_licenses_daily_states') }}

),

grid as (

    select
        c.date_day as date,
        t.type

    from {{ ref('inter_licenses_calendar') }} c
    cross join types t

),

base as (

    select
        g.date,
        g.type,

        coalesce(m.active_license_count, 0)
            as active_license_count,

        coalesce(m.inactive_license_count, 0)
            as inactive_license_count,

        coalesce(m.active_license_price, 0)
            as active_license_price

    from grid g

    left join {{ ref('inter_license_daily_metrics') }} m
      on g.date = m.date
     and g.type = m.type

)

select
    date,
    type,

    active_license_count,
    inactive_license_count,
    active_license_price,

    coalesce(
        active_license_count
        - lag(active_license_count)
            over(partition by type order by date),
        0
    ) as daily_active_diff,

    coalesce(
        active_license_price
        - lag(active_license_price)
            over(partition by type order by date),
        0
    ) as daily_price_diff,

    coalesce(
        inactive_license_count
        - lag(inactive_license_count)
            over(partition by type order by date),
        0
    ) as daily_inactive_diff

from base