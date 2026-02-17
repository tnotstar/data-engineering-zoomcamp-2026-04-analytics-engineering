with fhv_data as (
    select * from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    -- Grouping dimensions
    dz.zone AS revenue_zone,
    extract(year from f.pickup_datetime) as revenue_year,
    extract(month from f.pickup_datetime) as revenue_month,

    -- Calculation metrics
    count(f.dispatching_base_num) as total_monthly_trips,
    count(distinct f.dispatching_base_num) as unique_dispatching_bases

from fhv_data f
inner join dim_zones dz
    on f.pickup_location_id = dz.location_id
group by 1, 2, 3
