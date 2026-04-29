{{ config(materialized='view') }}

select 
    event_id,
    contract_id,
    device_mac,
    app_name,
    total_duration_seconds,
    total_duration_minutes,
    batch_date,
    view_year,
    view_month,
    view_day
from {{ ref('stg_iptv_logs_all') }}
where is_fraudulent = false