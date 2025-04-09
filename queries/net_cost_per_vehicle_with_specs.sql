-- max, min, avg and median will be calculated by Superset
-- using this query
with
  total_net_per_vehicle as (
    select
      stock_id
      ,sum(net_cost_price) as total_cost
    from
      fact_reconditioning_cost
    group by
      stock_id
  )
select
  c_per_v.stock_id
  ,c_per_v.total_cost
  ,d_v.make
  ,d_v.model
  ,d_v.door_count
  ,d_v.transmission_type
  ,d_v.fuel_type
from
  total_net_per_vehicle as c_per_v
join
  dim_vehicle as d_v
on
  c_per_v.stock_id = d_v.stock_id;