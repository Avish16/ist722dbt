{{ config(materialized='table') }}

select
    shipperid as shipperkey,
    shipperid,
    companyname,
    phone
from {{ source('northwind','shippers') }}
