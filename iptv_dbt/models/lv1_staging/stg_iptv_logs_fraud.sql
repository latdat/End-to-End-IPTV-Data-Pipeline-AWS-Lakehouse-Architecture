{{ config(materialized='view') }}

select *
from {{ ref('stg_iptv_logs_all') }}
where is_fraudulent = true