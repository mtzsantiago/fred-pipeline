with source as (
    select * from {{ source('raw', 'raw_fred_observations') }}
),

cleaned as (
    select
        series_id,
        obs_date,
        value,
        case
            when series_id = 'GDP'      then 'Gross Domestic Product'
            when series_id = 'CPIAUCSL' then 'CPI - Inflation'
            when series_id = 'UNRATE'   then 'Unemployment Rate'
            when series_id = 'FEDFUNDS' then 'Federal Funds Rate'
        end as series_name,
        case
            when series_id = 'GDP'      then 'Billions of Dollars'
            when series_id = 'CPIAUCSL' then 'Index 1982-1984=100'
            when series_id = 'UNRATE'   then 'Percent'
            when series_id = 'FEDFUNDS' then 'Percent'
        end as unit,
        ingested_at
    from source
    where value is not null
)

select * from cleaned
