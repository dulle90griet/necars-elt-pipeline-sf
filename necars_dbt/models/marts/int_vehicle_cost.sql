select
  *
from
  {{ ref('stg_source_vehicle') }} as vehicle
right join
  {{ ref('stg_source_cost') }} as cost
on cost.stock_number = SUBSTRING(vehicle.full_stock_number, 2)