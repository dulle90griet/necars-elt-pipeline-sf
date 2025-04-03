-- Get all distinct suppliers from the source tables    
    select recondition_supplier as supplier
    from {{ ref('stg_source_cost') }}
    where supplier is not null
union
    select vehicle_supplier
    from {{ ref('stg_source_vehicle') }}
    where vehicle_supplier is not null