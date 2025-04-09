-- Drills across the two fact tables to allow comparison
-- of net reconditioning cost per vehicle
-- in terms of original vehicle supplier and vehicle specs
-- Net Costs by Vehicle Supplier and Purchase Date, with Vehicle Specs
with
    f_c as (
        select
            stock_id
            ,sum(net_cost_price) as total_net_cost
        from
            fact_reconditioning_cost
        group by
            stock_id
    ),
    f_p as (
        select
            stock_id
            ,supplier_id
            ,purchase_invoice_date
        from
            fact_vehicle_purchase
    )
select
    f_c.stock_id
    ,f_p.purchase_invoice_date as vehicle_purchase_invoice_date
    ,f_c.total_net_cost
    ,f_p.supplier_id
    ,d_s.supplier_name
    ,d_v.make
    ,d_v.model
    ,d_v.door_count
    ,d_v.transmission_type
    ,d_v.fuel_type
from
    f_c
inner join
    f_p
on
    f_c.stock_id = f_p.stock_id
join
    dim_supplier as d_s
on
    f_p.supplier_id = d_s.supplier_id
join
    dim_vehicle as d_v
on
    f_p.stock_id = d_v.stock_id;
