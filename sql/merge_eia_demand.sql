MERGE INTO silver.eia_demand
USING silver.eia_demand_staging AS source
ON silver.eia_demand.period_utc = source.period_utc
   AND silver.eia_demand.respondent = source.respondent
   AND silver.eia_demand.type = source.type
WHEN MATCHED THEN UPDATE SET
    respondent_name = source.respondent_name,
    type_name = source.type_name,
    demand_mwh = source.demand_mwh,
    value_units = source.value_units
WHEN NOT MATCHED THEN INSERT (
    period_utc, respondent, respondent_name, type, type_name,
    demand_mwh, value_units
)
VALUES (
    source.period_utc, source.respondent, source.respondent_name,
    source.type, source.type_name, source.demand_mwh, source.value_units
);