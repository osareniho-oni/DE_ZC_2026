/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql
connection: duckdb-default

#-- Dependencies for lineage (declared below)
depends:
  - ingestion.trips
  - ingestion.payment_lookup

#-- Time-based incremental processing configured (time_interval)
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

#-- Output columns, primary keys, and basic checks defined below
columns:
  - name: trip_hash
    type: string
    description: surrogate hash for deduplication
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: pickup timestamp
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: taxi type (yellow/green)
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    description: fare amount
    checks:
      - name: non_negative

#-- Custom checks validating staging invariants
custom_checks:
  - name: staging_non_empty
    description: staging table should not be empty for the processed window
    query: |
      SELECT count(*) FROM staging.trips
    value: 1

@bruin */

-- Staging SELECT query implemented below
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH raw AS (
  SELECT *,
    -- ensure pickup/dropoff are cast to timestamps when possible
    pickup_datetime AS pickup_datetime_raw,
    dropoff_datetime AS dropoff_datetime_raw
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
), dedup AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY trip_hash ORDER BY extracted_at DESC) AS rn
  FROM raw
)
SELECT
  d.trip_hash,
  d.taxi_type,
  d.pickup_datetime_raw AS pickup_datetime,
  d.dropoff_datetime_raw AS dropoff_datetime,
  d.passenger_count,
  d.payment_type,
  d.fare_amount,
  d.extracted_at,
  p.payment_type_name
FROM dedup d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1
