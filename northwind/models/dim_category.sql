{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['category_id']) }} as category_sk,
  category_id,
  category_name,
  description
from {{ source('northwind', 'categories') }}
