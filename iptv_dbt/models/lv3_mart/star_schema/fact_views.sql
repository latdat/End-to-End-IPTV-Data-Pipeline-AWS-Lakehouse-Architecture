{{ config(
    materialized='incremental',
    unique_key='fact_view_key',
    incremental_strategy='delete+insert'
) }}

select
    -- Tạo khóa chính cho bảng Fact
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as fact_view_key,
    
    {{ dbt_utils.generate_surrogate_key(['contract_id']) }} as contract_key,
    {{ dbt_utils.generate_surrogate_key(['app_name']) }} as app_key,
    {{ dbt_utils.generate_surrogate_key(['device_mac']) }} as device_key,
    
    -- Thời gian
    cast(batch_date as integer) as date_key,
    
    -- Metrics
    total_duration_seconds,
    total_duration_minutes,
    total_duration_seconds as duration_seconds
    
from {{ ref('int_iptv_logs_enriched') }}

{% if is_incremental() %}
  where batch_date = replace('{{ var("batch_date") }}', '-', '')
{% endif %}