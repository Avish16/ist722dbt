{{ config(materialized='table') }}

select
    customerid as customerkey,
    customerid,
    companyname,
    contactname,
    contacttitle,
    address,
    city,
    region,
    postalcode,
    country,
    phone,
    fax
from {{ source('northwind', 'customers') }}

