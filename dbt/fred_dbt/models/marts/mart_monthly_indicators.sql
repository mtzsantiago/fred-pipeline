with staging as (
    select * from {{ ref('stg_fred_observations') }}
),

monthly as (
    select
        date_trunc('month', obs_date) as month,
        series_id,
        series_name,
        unit,
        avg(value)  as avg_value,
        min(value)  as min_value,
        max(value)  as max_value
    from staging
    group by 1, 2, 3, 4
)

select * from monthly
order by month, series_id
