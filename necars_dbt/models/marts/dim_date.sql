with date_spine as (
  {{ dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('01/01/2020', 'mm/dd/yyyy')",
    end_date="dateadd(year, 1, current_date())"
  ) }}
)
select
  -- date id
  cast(date_day as date) as date_id
  -- day id
  ,cast(year(date_id)||right('0'||dayofyear(date_id), 2) as int) as day_id
  -- day of month
  ,cast(dayofmonth(date_id) as int) as day_of_month
  -- day of week
  ,cast(dayofweek(date_id) as int) as day_of_week
  -- weekday short name
  ,cast(dayname(date_id) as varchar(3)) as weekday_short_name
  -- weekday name
  ,decode(day_of_week,
    0, 'Sunday',
    1, 'Monday',
    2, 'Tuesday',
    3, 'Wednesday',
    4, 'Thursday',
    5, 'Friday',
    6, 'Saturday'
  ) as weekday_name
  -- week id
  ,cast(year(date_id)||right('0'||week(date_id), 2) as int) as week_id
  -- week of year
  ,cast(weekofyear(date_id) as int) as week_of_year
  -- month id
  ,cast(year(date_id)||right('0'||month(date_id), 2) as int) as month
  -- month of year
  ,cast(month(date_id) as int) as month_of_year
  -- month short name
  ,cast(monthname(date_id) as varchar(3)) as month_short_name
  -- month name
  ,decode(month_of_year,
    1, 'January',
    2, 'February',
    3, 'March',
    4, 'April',
    5, 'May',
    6, 'June',
    7, 'July',
    8, 'August',
    9, 'September',
    10, 'October',
    11, 'November',
    12, 'December'
  ) as month_name
  -- year
  ,cast(year(date_id) as int) as year
  -- tax_year
  ,case
    when month_of_year < 4
      or (month_of_year = 4 and day_of_month < 6)
      then cast((year - 1)||'-'||year as varchar(9))
    else cast(year||'-'||(year + 1) as varchar(9))
  end as tax_year
  -- quarter_id
  ,cast(year(date_id)||right('0'||quarter(date_id), 2) as int) as quarter_id
  -- quarter_of_year
  ,cast(quarter(date_id) as int) as quarter_of_year
from date_spine