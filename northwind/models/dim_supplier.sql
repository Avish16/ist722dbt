{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
  supplier_id,
  company_name,
  contact_name,
  contact_title,
  country,
  city,
  region,
  postal_code,
  phone
from {{ source('northwind', 'suppliers') }}
