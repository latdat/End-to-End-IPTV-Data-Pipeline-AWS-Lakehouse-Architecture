{{ config(materialized='table') }}

with date_source as (
    select * from {{ ref('dates') }}
)

select
    -- Tạo date_key định dạng YYYYMMDD (số hoặc chuỗi) để join với Fact
    cast(replace(cast(date_day as varchar), '-', '') as integer) as date_key,
    
    date_day as full_date,
    month_name as month,
    day_of_week,
    day_of_month,
    week_of_year,
    quarter_of_year,
    year_number,
    
    -- Gán mặc định hoặc join với bảng lễ tết nếu có
    false as is_holiday 

from date_source