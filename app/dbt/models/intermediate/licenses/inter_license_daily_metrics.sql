--Aggregate counts and revenue.

select
    date,
    type,

    sum(case when renewable = true then 1 else 0 end)
        as active_license_count,

    sum(case when renewable = false then 1 else 0 end)
        as inactive_license_count,

    sum(case when renewable = true then price else 0 end)
        as active_license_price

from {{ ref('inter_licenses_daily_states') }}

group by date, type