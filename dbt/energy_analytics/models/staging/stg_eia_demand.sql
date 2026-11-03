SELECT
    period_utc,
    respondent,
    respondent_name,
    type,
    type_name,
    demand_mwh,
    value_units
FROM {{ source('silver', 'eia_demand') }}
WHERE demand_mwh IS NOT NULL