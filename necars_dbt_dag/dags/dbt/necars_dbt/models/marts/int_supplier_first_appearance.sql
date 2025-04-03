select
    -- Select each distinct supplier with their earliest appearance
    case
        when recondition_date < purchase_date
          or purchase_date is null
          then recondition_date
        else purchase_date
    end as first_appearance
    ,supplier
from {{ ref('int_distinct_supplier') }}
left join {{ ref('int_supplier_first_appearance_cost') }}
  on recondition_supplier = supplier
left join {{ ref('int_supplier_first_appearance_vehicle') }}
  on vehicle_supplier = supplier