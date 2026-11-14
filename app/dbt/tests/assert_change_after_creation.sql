-- A license change cannot happen before the license's creation date.
-- Same rule as rule_change_after_creation in app/validate_csv.py.
-- Returns the offending changes; the test fails if any row comes back.
select
    c.id as change_id,
    c.license_id,
    c.date,
    i.creation_date
from {{ source('licenses', 'license_changes') }} c
join {{ source('licenses', 'initial_licenses') }} i
    on i.id = c.license_id
where c.date < i.creation_date
