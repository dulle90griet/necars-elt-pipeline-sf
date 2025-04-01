select
  full_stock_number,
  in_stock_date,
  purchase_invoice_date,
  purchase_invoice_no,
  nominal_purchase_code,
  make,
  description as vehicle_description,
  supplier as vehicle_supplier,
  purchase_price_gbp,
  part_ex
from
  {{ source('necars_source', 'vehicle_purchase') }}