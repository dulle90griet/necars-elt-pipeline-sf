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
  ),
  supplier_vehicle_count as (
    select
      supplier_id
      ,count(supplier_id) as count
    from
      fact_vehicle_purchase
    group by
      supplier_id
    having
      count > 1 -- Filter for suppliers with at least two vehicle data points
  )
select
  f_c.stock_id
  ,f_p.purchase_invoice_date as vehicle_purchase_invoice_date
  ,f_c.total_net_cost
  ,f_p.supplier_id
  ,d_s.supplier_name
  ,svc.count as supplier_count
  ,d_v.make
  ,d_v.model
  ,d_v.door_count
  ,d_v.transmission_type
  ,d_v.fuel_type
from
  supplier_vehicle_count as svc
inner join
  f_p
on
  svc.supplier_id = f_p.supplier_id
inner join
  f_c
on
  f_p.stock_id = f_c.stock_id
join
  dim_supplier as d_s
on
  f_p.supplier_id = d_s.supplier_id
join
  dim_vehicle as d_v
on
  f_p.stock_id = d_v.stock_id;
