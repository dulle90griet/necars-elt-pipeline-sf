select
  stock_number,
  stock_type,
  date,
  description as recondition_description,
  quantity,
  gross_amount,
  vat,
  net_cost_price,
  net_extra_price,
  profit,
  nominal_code,
  sale_date,
  supplier as recondition_supplier,
  sage_ref
from
  {{ source('necars_source', 'reconditioning_cost') }}