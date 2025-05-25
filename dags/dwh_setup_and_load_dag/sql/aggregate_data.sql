-- airflow_project/dags/dwh_setup_and_load_dag/sql/aggregate_data.sql

TRUNCATE TABLE agg_yearly_city_temperature;

INSERT INTO agg_yearly_city_temperature (
    city_sk,
    year,
    avg_yearly_temp,
    min_monthly_temp,
    max_monthly_temp
)
SELECT
    fc.city_sk,
    d.year,
    ROUND(AVG(fc.average_temperature_celsius), 3) AS avg_yearly_temp,
    MIN(fc.average_temperature_celsius) AS min_monthly_temp,
    MAX(fc.average_temperature_celsius) AS max_monthly_temp
FROM
    fact_monthly_temperatures fc
JOIN
    dim_date d ON fc.date_sk = d.date_sk
GROUP BY
    fc.city_sk,
    d.year
ORDER BY
    fc.city_sk,
    d.year;