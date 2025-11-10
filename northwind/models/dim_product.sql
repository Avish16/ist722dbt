{{ config(materialized='table') }}

with src as (
  select
      product_id,
      product_name,
      supplier_id,
      category_id,
      quantity_per_unit,
      unit_price::decimal(18,2) as unit_price,
      units_in_stock,
      units_on_order,
      reorder_level,
      discontinued
  from {{ source('northwind', 'products') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
  product_id,
  product_name,
  supplier_id,
  category_id,
  quantity_per_unit,
  unit_price,
  units_in_stock,
  units_on_order,
  reorder_level,
  discontinued
from src
