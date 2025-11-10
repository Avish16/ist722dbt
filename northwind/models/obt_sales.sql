{{ config(materialized='view') }}

with f as (
  select * from {{ ref('fact_sales') }}
),

p as ( select product_id, product_sk, product_name, supplier_id, category_id
       from {{ ref('dim_product') }} ),
s as ( select supplier_id, supplier_sk, company_name as supplier_name
       from {{ ref('dim_supplier') }} ),
c as ( select category_id, category_sk, category_name
       from {{ ref('dim_category') }} ),
sh as ( select shipper_id, shipper_sk, shipper_name
        from {{ ref('dim_shipper') }} )

-- (Optionally join dim_customer / dim_employee / dim_date if you have them.)

select
  f.*,
  p.product_sk, p.product_name,
  c.category_sk, c.category_name,
  s.supplier_sk, s.supplier_name,
  sh.shipper_sk, sh.shipper_name
from f
left join p  using (product_id)
left join c  using (category_id)
left join s  using (supplier_id)
left join sh using (shipper_id)
