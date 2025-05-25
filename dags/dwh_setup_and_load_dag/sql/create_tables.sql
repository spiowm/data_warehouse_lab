-- airflow_project/dags/dwh_setup_and_load_dag/sql/create_tables.sql

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE,
    year SMALLINT,
    month SMALLINT,
    month_name VARCHAR(20),
    year_month INT,
    quarter SMALLINT,
    season VARCHAR(20),
    decade SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_city (
    city_sk INT PRIMARY KEY,
    city_name VARCHAR(100),
    country_name VARCHAR(100),
    latitude_val DECIMAL(9,6),
    longitude_val DECIMAL(9,6),
    continent_name VARCHAR(50),
    hemisphere VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS fact_monthly_temperatures (
    date_sk INT,
    city_sk INT,
    average_temperature_celsius DECIMAL(5,3),
    average_temperature_uncertainty DECIMAL(5,3),
    PRIMARY KEY (date_sk, city_sk),
    FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (city_sk) REFERENCES dim_city(city_sk) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS agg_yearly_city_temperature (
    city_sk INT,
    year SMALLINT,
    avg_yearly_temp DECIMAL(5,3),
    min_monthly_temp DECIMAL(5,3),
    max_monthly_temp DECIMAL(5,3),
    PRIMARY KEY (city_sk, year),
    FOREIGN KEY (city_sk) REFERENCES dim_city(city_sk) ON DELETE CASCADE ON UPDATE CASCADE
);