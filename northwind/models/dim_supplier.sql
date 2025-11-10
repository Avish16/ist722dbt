{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['supplierid']) }} as supplier_sk,
    supplierid,
    companyname as supplier_name,
    contactname,
    contacttitle,
    address,
    city,
    region,
    postalcode,
    country,
    phone,
    fax
from {{ source('northwind','suppliers') }}

