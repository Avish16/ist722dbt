{{ config(materialized='table') }}

with ord as (
  select
    orderid,
    customerid,
    employeeid,
    orderdate::date as orderdate,
    requireddate::date as requireddate,
    shippeddate::date as shippeddate,
    shipvia as shipperid,
    freight::decimal(18,2) as freight
  from {{ source('northwind', 'orders') }}
),

od as (
  select
    orderid,
    productid,
    unitprice::decimal(18,2) as unitprice,
    quantity::int as quantity,
    discount::decimal(9,4) as discount
  from {{ source('northwind', 'order_details') }}
),

calc as (
  select
    orderid,
    productid,
    unitprice,
    quantity,
    discount,
    unitprice * quantity as extended_price,
    unitprice * quantity * discount as discount_amount,
    unitprice * quantity - (unitprice * quantity * discount) as net_sales
  from od
)

select
  c.orderid,
  c.productid,
  o.customerid,
  o.employeeid,
  o.orderdate,
  o.requireddate,
  o.shippeddate,
  o.shipperid,
  o.freight,
  c.unitprice,
  c.quantity,
  c.discount,
  c.extended_price,
  c.discount_amount,
  c.net_sales
from calc c
join ord o on o.orderid = c.orderid
