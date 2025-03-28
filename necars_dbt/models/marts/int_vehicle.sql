select
  cast(substring(full_stock_number, 2) as int) as stock_id
  ,substring(full_stock_number, 1, 1) as stock_type
  ,make
  ,vehicle_description
from
  {{ ref('stg_source_vehicle') }} as src_vehicle
