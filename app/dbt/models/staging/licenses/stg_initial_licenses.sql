--Normalize initial licenses.
{{ config(materialized='view') }}
select
    id as license_id,
    cast(creation_date as date) as creation_date,
    renewable,
    price,
    type
from initial_licenses 