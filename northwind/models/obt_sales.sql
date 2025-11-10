{{ config(materialized='view') }}

with f as (
    select *
    from analytics.dbt_avish16_northwind.fact_sales
),

p as (
    select 
        productid,
        product_name,
        supplierid,
        categoryid
    from analytics.dbt_avish16_northwind.dim_product
),

s as (
    select 
        supplierid,
        supplier_name
    from analytics.dbt_avish16_northwind.dim_supplier
),

c as (
    select
        categoryid,
        categoryname as category_name
    from analytics.dbt_avish16_northwind.dim_category
),

sh as (
    select
        shipperid,
        companyname as shipper_name
    from analytics.dbt_avish16_northwind.dim_shipper
)

select
    f.*,
    p.product_name,
    c.category_name,
    s.supplier_name,
    sh.shipper_name

from f
left join p  on f.productid  = p.productid
left join c  on p.categoryid = c.categoryid
left join s  on p.supplierid = s.supplierid
left join sh on f.shipperid  = sh.shipperid
