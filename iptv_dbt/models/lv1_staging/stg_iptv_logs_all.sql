{{ config(materialized='view') }}

select
    event_id,
    contract_id,
    device_mac,
    app_name,
    total_duration_seconds,
    batch_date,
    round(total_duration_seconds / 60.0, 2)  as total_duration_minutes,
    left(batch_date, 4)                      as view_year,
    substring(batch_date, 5, 2)              as view_month,
    right(batch_date, 2)                     as view_day,
    is_fraudulent,
    fraud_reasons
from {{ source('staging', 'viewing_history') }}
