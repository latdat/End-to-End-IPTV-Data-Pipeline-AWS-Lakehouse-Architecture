select
    {{ dbt_utils.generate_surrogate_key(['batch_date', 'app_name']) }} as pk,
    batch_date,
    app_name,
    count(*)                                      as total_sessions,
    count(distinct contract_id)                   as unique_contracts,
    count(distinct device_mac)                    as unique_devices,
    sum(total_duration_seconds)                   as total_seconds,
    round(sum(total_duration_seconds)/3600.0, 2)  as total_hours,
    round(avg(total_duration_seconds)/60.0, 2)    as avg_minutes_per_session
from {{ ref('stg_viewing_history') }}
group by batch_date, app_name