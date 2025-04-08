select
    max(total_cost) as max_cost
    ,min(total_cost) as min_cost
    ,avg(total_cost) as mean_cost
    ,median(total_cost) as median_cost
from(
    select
        stock_id
        ,sum(net_cost_price) as total_cost
    from
        fact_reconditioning_cost
    group by
        stock_id
) total_cost_per_vehicle;