/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

#-- Asset: reports.trips_report
name: reports.trips_report

#-- Platform type: duckdb.sql
type: duckdb.sql
connection: duckdb-default

# -- Depends on staging.trips
depends:
  - staging.trips

#-- Materialization strategy chosen: time_interval (refresh by pickup_datetime date)
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # report is keyed by the pickup date
  incremental_key: pickup_datetime
  time_granularity: date

# -- Report columns and primary keys defined below
columns:
  - name: taxi_type
    type: string
    description: taxi type (yellow/green)
    primary_key: true
  - name: payment_type_name
    type: string
    description: human readable payment type
    primary_key: true
  - name: trip_date
    type: date
    description: pickup date
    primary_key: true
  - name: trip_count
    type: bigint
    description: number of trips
    checks:
      - name: non_negative
  - name: total_fare
    type: double
    description: sum of fare_amount
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  taxi_type,
  payment_type_name,
  CAST(pickup_datetime AS DATE) AS trip_date,
  count(*) AS trip_count,
  sum(fare_amount) AS total_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1,2,3
