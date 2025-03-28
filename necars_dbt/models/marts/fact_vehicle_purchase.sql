select
  cast(substring(s_v.full_stock_number, 2) as int) as stock_id
  ,d_s.supplier_id
  ,s_v.purchase_invoice_no as purchase_invoice_id
  ,date(s_v.purchase_invoice_date, 'dd/mm/yyyy hh24:mi') as purchase_invoice_date
  ,time(to_timestamp(s_v.purchase_invoice_date, 'dd/mm/yyyy hh24:mi')) as purchase_invoice_time
  ,s_v.nominal_purchase_code as nominal_code
  ,s_v.purchase_price_gbp
  ,s_v.part_ex
  ,date(s_v.in_stock_date, 'dd/mm/yyyy') as in_stock_date
from
  stg_source_vehicle as s_v
join
  dim_supplier as d_s
on
  s_v.vehicle_supplier = d_s.supplier_name