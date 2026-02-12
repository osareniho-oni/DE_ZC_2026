/* @bruin

# Staging asset: Clean, deduplicate, and enrich raw trip data
# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips

# Platform type for DuckDB SQL
type: duckdb.sql

# Dependencies - this asset depends on ingestion assets
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# Time-based incremental processing
materialization:
  type: table
  # time_interval strategy: delete rows in the time window, then insert new ones
  strategy: time_interval
  # Use pickup_datetime as the incremental key (matches ingestion logic)
  incremental_key: pickup_datetime
  # timestamp granularity since pickup_datetime is a timestamp
  time_granularity: timestamp

# Output columns with quality checks
columns:
  - name: trip_id
    type: string
    description: Unique identifier for the trip (generated from composite key)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
  - name: pickup_datetime
    type: timestamp
    description: Timestamp when the trip started
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Timestamp when the trip ended
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone ID for pickup location
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone ID for dropoff location
    checks:
      - name: not_null
  - name: passenger_count
    type: double
    description: Number of passengers in the vehicle
    checks:
      - name: non_negative
  - name: trip_distance
    type: double
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: fare_amount
    type: double
    description: Base fare amount
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total amount charged
  - name: payment_type
    type: integer
    description: Payment type ID
  - name: payment_type_name
    type: string
    description: Human-readable payment type name (enriched from lookup)
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted

# Custom check: verify no duplicate trips after deduplication
custom_checks:
  - name: no_duplicate_trips
    description: Ensure deduplication logic removed all duplicates
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT trip_id) AS duplicate_count
      FROM staging.trips
    value: 0

@bruin */

-- Staging query: Clean, deduplicate, and enrich
WITH deduplicated_trips AS (
  SELECT
    -- Generate a unique trip_id from composite key
    MD5(
      CONCAT(
        COALESCE(CAST(taxi_type AS VARCHAR), ''),
        COALESCE(CAST(pickup_datetime AS VARCHAR), ''),
        COALESCE(CAST(dropoff_datetime AS VARCHAR), ''),
        COALESCE(CAST(pickup_location_id AS VARCHAR), ''),
        COALESCE(CAST(dropoff_location_id AS VARCHAR), ''),
        COALESCE(CAST(fare_amount AS VARCHAR), '')
      )
    ) AS trip_id,
    taxi_type,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    CAST(payment_type AS INTEGER) AS payment_type,
    extracted_at,
    -- Use ROW_NUMBER to deduplicate (keep first occurrence)
    ROW_NUMBER() OVER (
      PARTITION BY 
        taxi_type,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        fare_amount
      ORDER BY extracted_at DESC
    ) AS row_num
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    -- Filter out invalid records
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND fare_amount >= 0
    AND trip_distance >= 0
)
SELECT
  t.trip_id,
  t.taxi_type,
  t.pickup_datetime,
  t.dropoff_datetime,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.passenger_count,
  t.trip_distance,
  t.fare_amount,
  t.total_amount,
  t.payment_type,
  COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
  t.extracted_at
FROM deduplicated_trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id
WHERE t.row_num = 1;  -- Keep only the first occurrence of each trip
