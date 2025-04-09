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
      ,part_ex
    from
      fact_vehicle_purchase
  )
select
  f_c.stock_id
  ,f_c.total_net_cost
  ,case
    when f_p.part_ex then 'Part Ex'
    else 'Auction'
  end as category
from
  f_c
inner join
  f_p
on
  f_c.stock_id = f_p.stock_id
order by f_c.stock_id;