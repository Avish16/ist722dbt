{{ config(materialized='table') }}

with f as (
    select *
    from analytics.dbt_avish16_northwind.fact_order_fulfillment
),

d_customer as (
    select *
    from analytics.dbt_avish16_northwind.dim_customer
),

d_employee as (
    select *
    from analytics.dbt_avish16_northwind.dim_employee
),

d_date as (
    select *
    from analytics.dbt_avish16_northwind.dim_date
)

select
    f.orderid,
    f.customerid,
    f.employeeid,

    -- ✅ Customer attributes
    d_customer.companyname     as customer_company,
    d_customer.contactname,
    d_customer.city            as customer_city,
    d_customer.country         as customer_country,

    -- ✅ Employee attributes
    concat(d_employee.firstname, ' ', d_employee.lastname) as employeenamefirstlast,
    d_employee.title          as employeetitle,

    -- ✅ Date
    d_date.date               as order_date,

    -- ✅ Fact fields
    f.orderdate,
    f.requireddate,
    f.shippeddate,
    f.quantity,
    f.totalorderamount,
    f.daysfromordertoshipped,
    f.daysfromordertorequired,
    f.shippedtorequireddelta,
    f.shippedontime

from f
left join d_customer ON f.customerid = d_customer.customerid
left join d_employee ON f.employeeid = d_employee.employeeid
left join d_date     ON f.orderdate = d_date.date


