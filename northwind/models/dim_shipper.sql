{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['shipper_id']) }} as shipper_sk,
  shipper_id,
  company_name as shipper_name,
  phone
from {{ source('northwind', 'shippers') }}
