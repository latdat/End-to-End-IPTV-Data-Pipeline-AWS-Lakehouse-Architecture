{{ config(
    materialized='table',
    dist='auto',
    sort='event_date'
) }}

with fact as (
    select * from {{ ref('fact_views') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_app as (
    select * from {{ ref('dim_app') }}
),

dim_device as (
    select * from {{ ref('dim_device') }}
),

dim_contract as (
    select * from {{ ref('dim_contract') }}
)

select
    -- Event Info
    f.fact_view_key,
    f.duration_seconds,
    f.duration_seconds / 60.0 as duration_minutes,
    
    -- Date Context
    d.full_date as event_date,
    d.day_of_week,
    d.month,
    d.year_number,
    d.quarter_of_year,
    
    -- Device Context
    dv.device_mac,
    dv.manufacturer,
    dv.country_code as device_origin,
    dv.device_type,
    
    -- App Context
    a.app_name,
    a.app_category,
    
    -- Customer Context
    c.contract_id,
    c.customer_segment

from fact f
left join dim_date d     on f.date_key = d.date_key
left join dim_device dv  on f.device_key = dv.device_key
left join dim_app a      on f.app_key = a.app_key
left join dim_contract c on f.contract_key = c.contract_key