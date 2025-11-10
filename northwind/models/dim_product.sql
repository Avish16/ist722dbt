{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_sk,
    productid,
    productname as product_name,
    supplierid,
    categoryid,
    quantityperunit,
    unitprice,
    unitsinstock,
    unitsonorder,
    reorderlevel,
    discontinued
from {{ source('northwind','products') }}

