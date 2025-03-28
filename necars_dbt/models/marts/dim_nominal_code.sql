with
  distinct_codes as (
      select nominal_code
      from stg_source_cost
    union
      select nominal_purchase_code
      from stg_source_vehicle
    order by nominal_code
  )
select
  nominal_code as code_id
  ,case code_id
    when 5210 then 'Labour'
    when 5300 then 'Margin Scheme Vehicle'
    when 5301 then 'Buyer''s Fee'
    when 5302 then 'Miscellaneous'
    when 5303 then 'Warranty'
    when 5304 then 'Delivery'
    when 5305 then 'Cherished Transfer / Road Fund Licence'
    when 5350 then 'Auction Charge'
    when 6002 then 'Sublet'
    when 6500 then 'Parts'
    else 'Unknown Code'
  end as code_description
from
  distinct_codes
