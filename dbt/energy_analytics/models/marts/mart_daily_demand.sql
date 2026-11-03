{{ config(materialized='table') }}

SELECT
    CAST(period_utc AS DATE) AS demand_date,
    respondent,
    COUNT(*) AS hourly_records,
    SUM(demand_mwh) AS total_demand_mwh,
    AVG(demand_mwh) AS avg_demand_mwh,
    MAX(demand_mwh) AS peak_demand_mwh,
    MIN(demand_mwh) AS min_demand_mwh

FROM {{ ref('fct_hourly_demand') }}

GROUP BY 1, 2