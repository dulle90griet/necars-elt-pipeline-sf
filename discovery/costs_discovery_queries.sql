use role necars_role;

with window_table as (
    select
        stock_number || ' ' || date || ' ' || description as composite_key,
        count(composite_key) over(partition by composite_key) as instance_count
    from
        reconditioning_cost
)
select
    *
from
    window_table
where instance_count > 1
order by instance_count desc;

select *
from reconditioning_cost
where stock_number = 33569
  and date = '30/06/2023 00:00';

with window_table as (
    select
        sage_ref,
        count(sage_ref) over(partition by sage_ref) as total
    from
        reconditioning_cost
)
select *
from window_table
--where total > 1
order by total desc;

select description,
    avg(net_cost_price)
from reconditioning_cost
group by description;

select description,
    count(*) as total
from reconditioning_cost
where description ilike '%warranty%'
  or description ilike '%platinum%'
group by description
order by 2 desc;

with counted as (
    select description,
        count(*) as total
    from reconditioning_cost
    where description ilike '%warranty%'
        or description ilike '%platinum%'
    group by description
    order by 2 desc
)
select description, total,
    sum(total) over() as cumulative_total
from counted;

select stock_number,
    count(stock_number) as total
from reconditioning_cost
group by stock_number
order by total desc;

select distinct stock_number, nominal_code
from reconditioning_cost
where stock_number = 34401;

select distinct
    description,
    nominal_code
from reconditioning_cost
order by nominal_code;

select count(*)
from reconditioning_cost
where description = 'BUYERS FEE';

select net_cost_price, profit
from reconditioning_cost
where net_cost_price != profit * -1;
