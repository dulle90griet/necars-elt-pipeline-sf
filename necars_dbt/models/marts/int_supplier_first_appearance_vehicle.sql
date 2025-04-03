-- Get each supplier's first appearance in the vehicle table
select
    row_number() over(
        partition by vehicle_supplier
        order by purchase_invoice_date
    ) as appearance
    ,purchase_invoice_date as purchase_date
    ,vehicle_supplier
from
    {{ ref('stg_source_vehicle') }}
qualify
    appearance = 1