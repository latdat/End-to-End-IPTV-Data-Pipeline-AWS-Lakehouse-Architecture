{{ config(materialized='view') }}

select *
from {{ ref('stg_iptv__logs_all') }}
where is_fraudulent = true