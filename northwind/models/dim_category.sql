{{ config(materialized='table') }}

select
    categoryid as categorykey,
    categoryid,
    categoryname,
    description
from {{ source('northwind', 'categories') }}
