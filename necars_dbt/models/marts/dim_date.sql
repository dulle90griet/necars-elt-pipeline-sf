with date_spine as (
  {{ dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('01/01/2020', ')",
    end_date="dateadd(year, 1, current_date())"
  ) }}
)
select
  -- date_id
  cast(day_date as date) as date_id,
  cast(year(date_id) as int) as year_id,
  cast(year(date_id)||right('0'||)) -- WAIT - CHECK QUARTER() IS REALLY A SQL FUNCTION
  cast(
    year(date_id)||right('0'||month(date_key), 2) as int
  ) as month_id,
  cast(month(date_id) as int) as month_of_year,
  cast(
    year(date_id)||right('0'||week(date_id), 2) as int
  ) as week_id,
  cast(
    weekofyear(date_id) as int
  ) as week_of_year,
  cast(dayofmonth(date_id) as int) as day_of_month,
  -- year
  -- month
  -- day
  -- month_name
  -- day_of_week
  -- day_name
  -- quarter
from date_spine