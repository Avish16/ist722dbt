{{ config(materialized='table') }}

with employees as (
    select * from {{ source('northwind','employees') }}
)

select
    employeeid as employeekey,
    employeeid,
    lastname,
    firstname,
    title,
    reportsto
from employees
