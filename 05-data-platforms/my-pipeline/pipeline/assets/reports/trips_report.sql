/* @bruin

# Reports asset: Aggregate trip data by date, taxi_type, and payment_type
# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

# Platform type for DuckDB SQL
type: duckdb.sql

# Dependency on staging asset
depends:
  - staging.trips

# Time-based incremental processing (consistent with staging)
materialization:
  type: table
  # time_interval strategy: rebuild only the relevant time window
  strategy: time_interval
  # Use pickup_date as the incremental key (date-level aggregation)
  incremental_key: pickup_date
  # date granularity since we're aggregating by date
  time_granularity: date

# Report columns with quality checks
columns:
  - name: pickup_date
    type: date
    description: Date of the trip (extracted from pickup_datetime)
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
  - name: payment_type_name
    type: string
    description: Payment type name
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative
      - name: positive
  - name: total_passengers
    type: double
    description: Total number of passengers across all trips
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: Total distance traveled in miles
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Total fare amount collected
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total amount charged (including all fees)
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: double
    description: Average trip distance in miles
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: double
    description: Average fare amount per trip
    checks:
      - name: non_negative

@bruin */

-- Reports query: Aggregate by date, taxi_type, and payment_type
SELECT
  CAST(pickup_datetime AS DATE) AS pickup_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(COALESCE(passenger_count, 0)) AS total_passengers,
  SUM(trip_distance) AS total_distance,
  SUM(fare_amount) AS total_fare_amount,
  SUM(total_amount) AS total_amount,
  AVG(trip_distance) AS avg_trip_distance,
  AVG(fare_amount) AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name
ORDER BY
  pickup_date,
  taxi_type,
  payment_type_name
