-- Get each supplier's first appearance in the cost table
select
    row_number() over(
        partition by recondition_supplier
        order by date
    ) as appearance
    ,date as recondition_date
    ,recondition_supplier
from
    {{ ref('stg_source_cost') }}
qualify
    appearance = 1