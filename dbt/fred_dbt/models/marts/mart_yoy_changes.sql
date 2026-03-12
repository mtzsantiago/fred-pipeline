-- depends_on: {{ ref('stg_fred_observations') }}

with staging as (
    select * from {{ ref('stg_fred_observations') }}
),

with_lag as (
    select
        obs_date,
        series_id,
        series_name,
        unit,
        value,
        lag(value, 12) over (
            partition by series_id
            order by obs_date
        ) as value_year_ago
    from staging
),

yoy as (
    select
        obs_date,
        series_id,
        series_name,
        unit,
        value,
        value_year_ago,
        case
            when series_id = 'FEDFUNDS' then
                round((value - value_year_ago)::numeric, 2)
            when abs(value_year_ago) < 0.5 then null
            else round(
                ((value - value_year_ago) / value_year_ago * 100)::numeric, 2
            )
        end as yoy_change_pct
    from with_lag
    where value_year_ago is not null
)

select * from yoy
order by obs_date, series_id
