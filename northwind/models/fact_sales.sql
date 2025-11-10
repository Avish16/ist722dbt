{{ config(materialized='table') }}

with ord as (
  select
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date::date           as order_date,
    o.required_date::date        as required_date,
    o.shipped_date::date         as shipped_date,
    o.ship_via                   as shipper_id,
    o.freight::decimal(18,2)     as freight
  from {{ source('northwind', 'orders') }} o
),

od as (
  select
    order_id,
    product_id,
    unit_price::decimal(18,2)          as unit_price,
    quantity::int                      as quantity,
    discount::decimal(9,4)             as discount
  from {{ source('northwind', 'order_details') }}
),

calc as (
  select
    od.order_id,
    od.product_id,
    od.unit_price,
    od.quantity,
    od.discount,
    -- measures
    (od.unit_price * od.quantity)                              as extended_price,
    (od.unit_price * od.quantity * od.discount)                as discount_amount,
    (od.unit_price * od.quantity) - (od.unit_price * od.quantity * od.discount) as net_sales
  from od
),

joined as (
  select
    c.order_id,
    c.product_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.shipper_id,
    o.freight,

    c.unit_price,
    c.quantity,
    c.discount,
    c.extended_price,
    c.discount_amount,
    c.net_sales
  from calc c
  join ord o on o.order_id = c.order_id
)

select
  -- degenerate / natural keys
  order_id,
  product_id,
  customer_id,
  employee_id,
  shipper_id,

  -- date degenerate keys (or replace with date surrogate keys if you have dim_date)
  order_date,
  required_date,
  shipped_date,

  -- measures
  unit_price,
  quantity,
  discount,
  extended_price,
  discount_amount,
  net_sales,
  freight
from joined
