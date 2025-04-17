select
  cast(substring(full_stock_number, 2) as int) as stock_id
  ,substring(full_stock_number, 1, 1) as stock_type
  ,case
    when make ilike 'bmw' then 'BMW'
    when make ilike 'ds%' then 'DS Automobiles'
    when make ilike 'skoda' then 'Skoda'
    else make
  end as make
  ,vehicle_description
from
  {{ ref('stg_source_vehicle') }} as src_vehicle
