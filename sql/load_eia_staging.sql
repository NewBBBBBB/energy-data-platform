TRUNCATE TABLE silver.eia_demand_staging;

COPY silver.eia_demand_staging
FROM 's3://eia-demand-data/silver/eia/demand/'
IAM_ROLE default
FORMAT AS PARQUET;