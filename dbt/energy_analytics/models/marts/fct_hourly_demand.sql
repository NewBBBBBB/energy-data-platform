{{ config(materialized='table') }}

SELECT
    period_utc,
    respondent,
    demand_mwh
FROM {{ ref('stg_eia_demand') }}