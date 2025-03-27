select
    -- Generate supplier_ids in order of first appearance
    row_number() over (order by first_appearance) as supplier_id,
    supplier as supplier_name
from (
    with
        s as (
            -- Get all distinct suppliers from the source tables
                select recondition_supplier as supplier
                from stg_source_cost
                where supplier is not null
            union
                select vehicle_supplier
                from stg_source_vehicle
                where vehicle_supplier is not null
        ),
        c as (
            -- Get each supplier's first appearance in the cost table
            select
                row_number() over(
                    partition by recondition_supplier
                    order by date
                ) as appearance,
                date as recondition_date,
                recondition_supplier
            from
                stg_source_cost
            qualify
                appearance = 1
        ),
        v as (
            -- Get each supplier's first appearance in the vehicle table
            select
                row_number() over(
                    partition by vehicle_supplier
                    order by purchase_invoice_date
                ) as appearance,
                purchase_invoice_date as purchase_date,
                vehicle_supplier
            from
                stg_source_vehicle
            qualify
                appearance = 1
        )
    select
        -- Retain the suppliers with their earliest appearances
        case
            when recondition_date < purchase_date
              or purchase_date is null
              then recondition_date
            else purchase_date
        end as first_appearance,
        supplier
    from s
    left join c
      on recondition_supplier = supplier
    left join v
      on vehicle_supplier = supplier
) supplier_appearance