with grid as (

    select
        c.date_day as date,
        l.license_id

    from {{ ref('inter_licenses_calendar') }} c
    join {{ ref('stg_initial_licenses') }} l
      on c.date_day >= l.creation_date

),

joined as (

    select
        g.license_id,
        g.date,
        s.renewable,
        s.type,
        s.price,

        row_number() over (
            partition by g.license_id, g.date
            order by s.date desc, s.id desc
        ) rn

    from grid g

    left join {{ ref('inter_state_licenses') }} s
        on s.license_id = g.license_id
       and s.date <= g.date

)

select
    license_id,
    date,
    renewable,
    type,
    price

from joined
where rn = 1
  and renewable is not null