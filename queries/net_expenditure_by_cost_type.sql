select
  sum(f_c.net_cost_price) as net_expenditure
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
from
  fact_reconditioning_cost as f_c
join
  dim_nominal_code as d_n
on
  nominal_code = code_id
group by
  cost_type;