{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['device_mac']) }} as device_key,
    device_mac,
    mac_oui as oui_prefix,
    device_manufacturer as manufacturer,
    device_country as country_code,
    'SmartTV' as device_type -- Giá trị mặc định, có thể update logic từ seed file sau
from {{ ref('int_iptv_logs_enriched') }}
where device_mac is not null