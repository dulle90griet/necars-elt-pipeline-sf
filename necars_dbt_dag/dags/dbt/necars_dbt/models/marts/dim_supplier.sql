select
    -- Generate supplier_ids in order of first appearance
    row_number() over (order by first_appearance) as supplier_id
    ,supplier as supplier_name
from {{ ref('int_supplier_first_appearance') }}