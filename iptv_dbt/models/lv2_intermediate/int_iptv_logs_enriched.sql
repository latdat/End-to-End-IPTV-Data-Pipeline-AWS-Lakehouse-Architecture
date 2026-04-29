{{ config(
    materialized='table' 
) }}

with clean_logs as (
    select * from {{ ref('stg_iptv_logs') }}
),

app_mapping as (
    select * from {{ ref('app_segments') }}
),

oui_mapping as (
    select * from {{ ref('oui_lookup') }}
),

enriched_logs as (
    select
        l.event_id,
        l.contract_id,
        l.device_mac,
        left(l.device_mac, 6) as mac_oui, 
        l.app_name,
        l.total_duration_seconds,
        l.total_duration_minutes,
        l.batch_date,
        l.view_year,
        l.view_month,
        l.view_day,

        -- Thông tin làm giàu (Enriched) từ App Segments
        coalesce(a.Segment, 'Uncategorized') as app_segment,

        -- Thông tin làm giàu (Enriched) từ OUI Lookup
        coalesce(o.manufacturer, 'Unknown Manufacturer') as device_manufacturer,
        coalesce(o.country_code, 'Unknown') as device_country
        

    from clean_logs l
    
    -- Join để lấy phân khúc App
    left join app_mapping a
        on l.app_name = a.Apps
        
    -- Join để lấy thông tin thiết bị dựa trên 6 ký tự đầu của MAC
    left join oui_mapping o
        on left(l.device_mac, 6) = o.oui_hex
)

select * from enriched_logs

-- dbt seed rồi mới dbt run -s int_iptv_logs_enriched