select
  s_c.sage_ref
  ,s_c.stock_number as stock_id
  ,d_s.supplier_id
  ,date(s_c.date, 'dd/mm/yyyy hh24:mi') as payment_date
  ,time(right(s_c.date, 4)) as payment_time
  ,s_c.nominal_code
  ,case
    when s_c.nominal_code in (5302, 5303)
      then s_c.recondition_description
  end as description
  ,s_c.quantity
  ,s_c.gross_amount
  ,s_c.vat
  ,s_c.net_cost_price
  ,s_c.net_extra_price
  ,s_c.profit
from
  stg_source_cost as s_c
join
  dim_supplier as d_s
  on s_c.recondition_supplier = d_s.supplier_name
join
  dim_nominal_code as d_n
  on s_c.nominal_code = d_n.code_id
