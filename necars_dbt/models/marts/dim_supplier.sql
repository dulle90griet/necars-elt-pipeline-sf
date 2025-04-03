select
    -- Generate supplier_ids in order of first appearance
    row_number() over (order by first_appearance) as supplier_id
    ,supplier as supplier_name
from (
    select
        -- Retain the suppliers with their earliest appearances
        case
            when recondition_date < purchase_date
              or purchase_date is null
              then recondition_date
            else purchase_date
        end as first_appearance
        ,supplier
    from {{ ref('int_distinct_supplier') }} as s
    left join {{ ref('int_supplier_first_appearance_cost') }} as c
      on recondition_supplier = supplier
    left join {{ ref('int_supplier_first_appearance_vehicle') }} as v
      on vehicle_supplier = supplier
) supplier_appearance