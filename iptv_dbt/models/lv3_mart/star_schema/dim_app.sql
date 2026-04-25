{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['app_name']) }} as app_key,
    app_name,
    app_segment as app_category
from {{ ref('int_iptv_logs_enriched') }}
where app_name is not null