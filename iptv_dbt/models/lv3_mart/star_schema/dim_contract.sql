{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['contract_id']) }} as contract_key,
    contract_id,
    'Standard' as customer_segment  -- Giả sử tất cả khách hàng đều thuộc phân khúc "Standard" vì không có thông tin chi tiết hơn
from {{ ref('int_iptv_logs_enriched') }}
where contract_id is not null