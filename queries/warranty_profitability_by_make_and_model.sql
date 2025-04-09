-- Warranty Profitability by Make and Model
select
    f_c.stock_id
    ,f_c.nominal_code
    ,f_c.description
    ,f_c.profit
    ,d_v.make
    ,d_v.model
from
    fact_reconditioning_cost as f_c
join
    dim_vehicle as d_v
on
    f_c.stock_id = d_v.stock_id
where
    nominal_code = 5303 -- Warranty
    or (
        nominal_code = 5302 -- extract warranties filed as 'Extras'
        and (
            f_c.description ilike '%platinum%'
            or f_c.description ilike '%plan%'
            or f_c.description ilike '%warrant%'
        )
    );