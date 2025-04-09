-- supplies data for a graph with lines for each cost category
-- plus a line totalling all costs
select
    date_trunc('month', f_c.payment_date) as payment_date
    ,case
        when f_c.nominal_code = 5302 -- handle 'Miscellaneous' codes
            then case
                when f_c.description ilike '%platinum%'
                  or f_c.description ilike '%plan%'
                  or f_c.description ilike '%warrant%'
                    then 'Warranty'
                when f_c.description ilike '%parts%'
                    then 'Parts'
                when f_c.description ilike '%sublet%'
                    then 'Sublet'
                when f_c.description ilike '%auction%'
                    then 'Auction Charge'
                when f_c.description ilike '%cherished%'
                  or f_c.description ilike '%transfer%'
                    then 'Cherished Transfer'
                when f_c.description ilike '%tax%'
                  or f_c.description ilike '%licen%'
                    then 'Road Fund Licence'
                when f_c.description ilike '%deliv%'
                    then 'Delivery'
                else 'Miscellaneous'
            end
        when f_c.nominal_code = 5305 -- separate 'Cherished Transfer' from 'Road Fund Licence'
            then f_c.description
        else d_n.code_description
    end as cost_type
    ,sum(f_c.net_cost_price) as total_cost
from
    fact_reconditioning_cost as f_c
join
    dim_nominal_code as d_n
  on
    nominal_code = code_id
join
    dim_date as d_d
  on
    f_c.payment_date = d_d.date_id
group by
    date_trunc('month', f_c.payment_date)
    ,cost_type

union all

select
    date_trunc('month', payment_date) as payment_date
    ,'TOTAL' as cost_type
    ,sum(net_cost_price) as total_cost
from
    fact_reconditioning_cost
group by
    date_trunc('month', payment_date)

order by
    payment_date
    ,case when cost_type = 'TOTAL' then 'ZZZ' else cost_type end;