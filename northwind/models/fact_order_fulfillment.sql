{{ config(materialized='table') }}

with ord as (
  select
    orderid,
    customerid,
    employeeid,
    shipname,
    shipaddress,
    shipcity,
    shipregion,
    shippostalcode,
    shipcountry,
    shipvia,
    freight,
    orderdate::date      as orderdate,
    requireddate::date   as requireddate,
    shippeddate::date    as shippeddate
  from {{ source('northwind','orders') }}
),

calc as (
  select
    od.orderid,
    sum(od.quantity) as quantity,
    sum(od.unitprice * od.quantity * (1 - od.discount)) as totalorderamount
  from {{ source('northwind','order_details') }} od
  group by od.orderid
),

final as (
  select
    o.orderid,
    o.customerid,
    o.employeeid,
    o.shipname,
    o.shipaddress,
    o.shipcity,
    o.shipregion,
    o.shippostalcode,
    o.shipcountry,
    o.shipvia,
    o.freight,
    o.orderdate,
    o.requireddate,
    o.shippeddate,

    c.quantity,
    c.totalorderamount,

    datediff('day', o.orderdate, o.shippeddate) as daysfromordertoshipped,
    datediff('day', o.orderdate, o.requireddate) as daysfromordertorequired,
    datediff('day', o.requireddate, o.shippeddate) as shippedtorequireddelta,
    case when o.shippeddate <= o.requireddate then 'Y' else 'N' end as shippedontime
  from ord o
  left join calc c on o.orderid = c.orderid
)

select * from final
