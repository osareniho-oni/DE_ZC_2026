# NYC Taxi Data Pipeline - Kestra Workflow Orchestration

## 📋 Project Overview
This project implements a production-grade data pipeline for the [Data Engineering Zoomcamp 2026 - Module 2 Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/02-workflow-orchestration/homework.md) using **Kestra Open Source**.

The pipeline ingests NYC Taxi trip data (Yellow and Green taxis) from parquet files into Google Cloud Storage (GCS) and loads them into BigQuery with proper schema management and deduplication.

## 🏗️ Architecture

### Workflow Structure
The pipeline uses a **modular subflow architecture** for better maintainability and reusability:

```
nyc_taxi_parent (parent flow)
├── ingest_to_gcs (subflow)
│   ├── Download parquet from NYC TLC
│   ├── Check if file exists in GCS
│   └── Upload to GCS (partitioned by year/month)
└── load_to_bq (subflow)
    ├── Create main table (if not exists)
    ├── Create external table (parquet)
    ├── Create temp table with unique IDs
    ├── MERGE into main table (deduplication)
    └── Cleanup temp tables
```

## 🔑 Key Design Decisions

### 1. **Subflow Architecture vs Monolithic**
**My Implementation:**
- Split into 3 separate workflows: `nyc_taxi_parent`, `ingest_to_gcs`, `load_to_bq`
- Each subflow is independently testable and reusable

**Tutorial Implementation (08_gcp_taxi.yaml, 09_gcp_taxi_scheduled.yaml):**
- Single monolithic workflow with all tasks in one file
- Uses CSV format instead of Parquet
- Uses KV store for configuration

**Tradeoff:**
- ✅ **Pros:** Better separation of concerns, easier debugging, reusable components
- ⚠️ **Cons:** More files to manage, slightly more complex orchestration

### 2. **GCP Authentication: Direct Mount vs KV Store**
**My Implementation:**
- Service account JSON mounted directly via Docker volume
- Uses `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- No KV store configuration needed

**Tutorial Implementation:**
- Uses Kestra's KV (Key-Value) store for credentials
- Requires `{{kv('GCP_CREDS')}}` in workflow files

**Tradeoff:**
- ✅ Direct mount is simpler for local development
- ✅ No need to configure KV store
- ⚠️ KV store is better for production (centralized secrets management)

### 3. **Data Format: Parquet vs CSV**
**My Implementation:**
- Uses **Parquet files** from `d37ci6vzurychx.cloudfront.net/trip-data/`
- More efficient storage and faster BigQuery loading

**Tutorial Implementation:**
- Uses **CSV files** from GitHub releases
- Requires decompression (gunzip)

**Tradeoff:**
- ✅ Parquet is columnar, compressed, and schema-aware
- ✅ No decompression step needed
- ⚠️ Parquet files are larger to download initially

### 4. **Explicit Type Casting**
**My Implementation:**
```sql
CAST(trip_distance AS NUMERIC) AS trip_distance,
CAST(fare_amount AS NUMERIC) AS fare_amount,
CAST(trip_type AS STRING) AS trip_type
```
- Explicit CAST for all numeric and string columns
- Prevents FLOAT64 → NUMERIC type mismatch errors

**Tutorial Implementation:**
- Uses `SELECT *` with implicit type inference
- Works for CSV but can fail with Parquet

**Tradeoff:**
- ✅ Type-safe, prevents runtime errors
- ⚠️ More verbose SQL

### 5. **Backfill Control**
**My Implementation:**
- Explicit `backfill` boolean input in `ingest_to_gcs`
- Checks if file exists in GCS before downloading
- Skips existing files unless `backfill: true`

**Tutorial Implementation:**
- Uses execution labels (`backfill:true`) for tracking
- No explicit skip logic

**Tradeoff:**
- ✅ Clear operational intent
- ✅ Prevents accidental re-downloads
- ⚠️ Requires manual backfill flag setting

### 6. **Partitioning Strategy**
**Both Implementations:**
- Partition by DATE(pickup_datetime)
- Cluster by PULocationID, DOLocationID

**My Enhancement:**
- GCS files also partitioned: `yellow/year=2019/month=01/`
- Matches BigQuery partitioning for consistency

### 7. **Deduplication Strategy**
**Both Implementations:**
- MD5 hash of key columns (VendorID, pickup/dropoff times, locations)
- MERGE operation with `WHEN NOT MATCHED THEN INSERT`

**Result:**
- ✅ Idempotent: Running the same data twice won't create duplicates
- ✅ Safe for backfills and retries

### 8. **Scheduling**
**My Implementation:**
```yaml
triggers:
  - id: yellow_monthly
    cron: "0 2 1 * *"  # 2 AM on 1st of month
    inputs:
      start_year: "{{ now().minusMonths(1).year }}"
      end_year: "{{ now().minusMonths(1).year }}"
      months: ["{{ now().minusMonths(1).format('MM') }}"]
```

**Tutorial Implementation:**
```yaml
triggers:
  - id: green_schedule
    cron: "0 9 1 * *"  # 9 AM on 1st of month
```

**Difference:**
- My implementation processes previous month's data
- Tutorial uses `trigger.date` variable

## 📁 Project Files

| File | Purpose |
|------|---------|
| `nyc_taxi_parent.yaml` | Parent orchestration workflow with year/month loops |
| `ingest_to_gcs.yaml` | Downloads parquet files and uploads to GCS |
| `load_to_bq.yaml` | Loads data into BigQuery with schema management |
| `08_gcp_taxi.yaml` | Tutorial reference (CSV-based, monolithic) |
| `09_gcp_taxi_scheduled.yaml` | Tutorial reference (scheduled version) |

## 🚀 Setup & Installation

### Prerequisites
1. **Kestra Open Source** installed and running
2. **GCP Project** with billing enabled
3. **Docker & Docker Compose** installed

### Step 1: Create GCP Service Account

In GCP Console → IAM & Admin → Service Accounts:

1. **Create Service Account:**
   - Name: `kestra-sa`
   - Description: "Service account for Kestra workflows"

2. **Grant Roles:**
   - `BigQuery Admin` (or `BigQuery Data Editor` + `BigQuery Job User`)
   - `Storage Admin` (or `Storage Object Admin`)

3. **Create & Download Key:**
   - Click the service account → Keys tab
   - Add key → Create new key → JSON
   - Download the file (e.g., `kestra-gcp-sa.json`)

⚠️ **Security Note:** Never commit this file to Git! Add it to `.gitignore`

### Step 2: Setup GCP Resources

Create the required GCP resources:

```bash
# Set your project ID
export PROJECT_ID="<GCP_PROJECT_ID>"

# Create BigQuery dataset
bq mk --dataset --location=US ${PROJECT_ID}:wk1_tf_dataset

# Create GCS bucket
gsutil mb -l US gs://wk1-tf-bucket
```

### Step 3: Configure Kestra with GCP Credentials

**Option A: Docker Volume Mount (Recommended for Local Development)**

1. **Create folder structure:**
```bash
project-root/
├── docker-compose.yml
└── gcp/
    └── credentials/
        └── kestra-gcp-sa.json
```

2. **Update `docker-compose.yml`:**
```yaml
services:
  kestra:
    image: kestra/kestra:latest
    container_name: kestra
    ports:
      - "8080:8080"
    volumes:
      - ./gcp/credentials:/secrets
    environment:
      GOOGLE_APPLICATION_CREDENTIALS: /secrets/kestra-gcp-sa.json
```

3. **Restart Kestra:**
```bash
docker-compose down
docker-compose up -d
```

4. **Verify credentials are mounted:**
```bash
docker exec -it kestra bash
ls /secrets
echo $GOOGLE_APPLICATION_CREDENTIALS
```

You should see:
```
kestra-gcp-sa.json
/secrets/kestra-gcp-sa.json
```

**Option B: Kestra KV Store (Tutorial Method)**

If you prefer using Kestra's Key-Value store:

1. Navigate to Kestra UI → Namespaces → `zoomcamp`
2. Add KV pairs:
   - `GCP_CREDS`: (paste entire JSON content)
   - `GCP_PROJECT_ID`: `<GCP_PROJECT_ID>`
   - `GCP_DATASET`: `wk1_tf_dataset`
   - `GCP_BUCKET_NAME`: `wk1-tf-bucket`
   - `GCP_LOCATION`: `US`

3. Update workflow files to use `{{kv('GCP_CREDS')}}` instead of environment variables

### Step 4: Deploy Workflows to Kestra

1. Navigate to Kestra UI: `http://localhost:8080`
2. Go to Flows → Create Flow
3. Upload or paste the workflow files:
   - `main.yaml` (parent workflow)
   - `ingest_to_gcs.yaml` (subflow)
   - `load_to_bq.yaml` (subflow)

## 🎯 How to Run

### Manual Execution
1. Navigate to Kestra UI → Flows → `zoomcamp.nyc_taxi_parent`
2. Click "Execute"
3. Set inputs:
   - `taxi`: yellow or green
   - `start_year`: 2019
   - `end_year`: 2021
   - `months`: (optional) e.g., `["01","02","03"]`

### Scheduled Execution
The workflow automatically runs monthly:
- **Yellow taxi:** 2 AM on the 1st of each month
- **Green taxi:** 3 AM on the 1st of each month

Processes the previous month's data automatically.

## 🐛 Common Issues & Solutions

### Issue 1: Template Variable Not Resolved
**Error:** `Invalid dataset ID "{{ vars.dataset }}.{{ inputs"`

**Solution:** Avoid nested template variables. Use direct expressions:
```yaml
# ❌ Bad
variables:
  table: "{{ vars.dataset }}.{{ inputs.taxi }}_tripdata"
sql: "CREATE TABLE {{ vars.table }}"

# ✅ Good
sql: "CREATE TABLE {{ vars.dataset }}.{{ inputs.taxi }}_tripdata"
```

### Issue 2: Type Mismatch (FLOAT64 → NUMERIC)
**Error:** `Value has type FLOAT64 which cannot be inserted into column trip_distance`

**Solution:** Add explicit CAST in temp table creation:
```sql
CAST(trip_distance AS NUMERIC) AS trip_distance
```

### Issue 3: Year Range Off by One
**Error:** Selecting 2019-2019 processes 2019 and 2020

**Solution:** Kestra's `range()` is inclusive on both ends:
```yaml
# ❌ Bad
values: "{{ range(inputs.start_year, inputs.end_year + 1) }}"

# ✅ Good
values: "{{ range(inputs.start_year, inputs.end_year) }}"
```

## 📊 Querying the Data

### Check Loaded Data
```sql
SELECT 
  EXTRACT(YEAR FROM tpep_pickup_datetime) as year,
  EXTRACT(MONTH FROM tpep_pickup_datetime) as month,
  COUNT(*) as trip_count
FROM `<GCP_PROJECT_ID>.wk1_tf_dataset.yellow_tripdata`
GROUP BY year, month
ORDER BY year, month;
```

### Verify No Duplicates
```sql
SELECT 
  unique_row_id,
  COUNT(*) as count
FROM `<GCP_PROJECT_ID>.wk1_tf_dataset.yellow_tripdata`
GROUP BY unique_row_id
HAVING COUNT(*) > 1;
```

### Delete Unwanted Data (e.g., 2022)
```sql
DELETE FROM `<GCP_PROJECT_ID>.wk1_tf_dataset.yellow_tripdata`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2022;
```

## 🎯 Homework Alignment

This implementation aligns with the Data Engineering Zoomcamp Module 2 homework requirements while introducing production-grade enhancements:

| Requirement | Implementation |
|-------------|----------------|
| Workflow orchestration | ✅ Kestra workflows |
| GCS ingestion | ✅ Parquet files partitioned by year/month |
| BigQuery loading | ✅ Schema-aware with deduplication |
| Scheduling | ✅ Monthly cron triggers |
| Backfilling | ✅ Year/month range support |
| Idempotency | ✅ MERGE operations prevent duplicates |

## 🔄 Differences from Tutorial

| Aspect | Tutorial | My Implementation |
|--------|----------|-------------------|
| Architecture | Monolithic | Modular subflows |
| Data format | CSV (gzipped) | Parquet |
| Configuration | KV store | Hardcoded variables |
| Type handling | Implicit | Explicit CAST |
| Backfill control | Labels | Boolean input |
| GCS structure | Flat | Partitioned (year/month) |
| Error handling | Basic | Type-safe with explicit casts |

## 📝 Lessons Learned

1. **Template Resolution:** Avoid nested variables in Kestra; use direct expressions
2. **Type Safety:** Always CAST numeric columns when loading from Parquet to BigQuery
3. **Idempotency:** MERGE operations are crucial for safe backfills
4. **Partitioning:** Align GCS and BigQuery partitioning for consistency
5. **Modularity:** Subflows improve testability and reusability

## 🙏 Acknowledgments

- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by DataTalks.Club
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Kestra Documentation](https://kestra.io/docs)