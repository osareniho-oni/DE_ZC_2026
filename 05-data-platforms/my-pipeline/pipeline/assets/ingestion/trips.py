"""@bruin

# Asset name following the ingestion schema pattern
name: ingestion.trips

# Asset type for Python ingestion
type: python

# Connection for materialization
connection: duckdb-default

# Python image version
image: python:3.11

# Materialization configuration for Python assets
# Using append strategy for raw ingestion (duplicates handled in staging)
materialization:
  type: table
  strategy: append

# Output columns with types and descriptions
columns:
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
  - name: pickup_datetime
    type: timestamp
    description: Timestamp when the trip started
  - name: dropoff_datetime
    type: timestamp
    description: Timestamp when the trip ended
  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone ID for pickup location
  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone ID for dropoff location
  - name: passenger_count
    type: double
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: double
    description: Trip distance in miles
  - name: fare_amount
    type: double
    description: Base fare amount
  - name: extra
    type: double
    description: Extra charges
  - name: mta_tax
    type: double
    description: MTA tax
  - name: tip_amount
    type: double
    description: Tip amount
  - name: tolls_amount
    type: double
    description: Tolls amount
  - name: improvement_surcharge
    type: double
    description: Improvement surcharge
  - name: total_amount
    type: double
    description: Total amount charged
  - name: payment_type
    type: double
    description: Payment type ID
  - name: congestion_surcharge
    type: double
    description: Congestion surcharge
  - name: airport_fee
    type: double
    description: Airport fee
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted

@bruin"""

import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE for date range
    - BRUIN_VARS for pipeline variables (taxi_types)
    
    Returns a DataFrame with raw trip data.
    """
    # Get date range from Bruin environment variables
    start_date_str = os.environ.get('BRUIN_START_DATE')
    end_date_str = os.environ.get('BRUIN_END_DATE')
    
    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Get taxi_types from pipeline variables
    bruin_vars = os.environ.get('BRUIN_VARS', '{}')
    variables = json.loads(bruin_vars)
    taxi_types = variables.get('taxi_types', ['yellow', 'green'])
    
    print(f"Fetching data from {start_date_str} to {end_date_str}")
    print(f"Taxi types: {taxi_types}")
    
    # Base URL for NYC TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Generate list of files to fetch
    all_dataframes = []
    current_date = start_date
    
    while current_date < end_date:
        year_month = current_date.strftime('%Y-%m')
        
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = base_url + filename
            
            print(f"Fetching: {url}")
            
            try:
                # Fetch parquet file
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                
                # Read parquet data into DataFrame
                df = pd.read_parquet(url)
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.utcnow()
                
                # Standardize column names (different taxi types may have slight variations)
                # Map to consistent column names
                column_mapping = {
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                }
                
                df = df.rename(columns=column_mapping)
                
                # Select and order columns consistently
                expected_columns = [
                    'taxi_type',
                    'pickup_datetime',
                    'dropoff_datetime',
                    'pickup_location_id',
                    'dropoff_location_id',
                    'passenger_count',
                    'trip_distance',
                    'fare_amount',
                    'extra',
                    'mta_tax',
                    'tip_amount',
                    'tolls_amount',
                    'improvement_surcharge',
                    'total_amount',
                    'payment_type',
                    'congestion_surcharge',
                    'airport_fee',
                    'extracted_at'
                ]
                
                # Keep only columns that exist in the DataFrame
                available_columns = [col for col in expected_columns if col in df.columns]
                df = df[available_columns]
                
                # Add missing columns with None values
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = None
                
                # Reorder to match expected schema
                df = df[expected_columns]
                
                all_dataframes.append(df)
                print(f"  ✓ Fetched {len(df)} rows from {filename}")
                
            except requests.exceptions.RequestException as e:
                print(f"  ✗ Failed to fetch {filename}: {e}")
                # Continue with other files even if one fails
                continue
            except Exception as e:
                print(f"  ✗ Error processing {filename}: {e}")
                continue
        
        # Move to next month
        current_date += relativedelta(months=1)
    
    # Concatenate all DataFrames
    if not all_dataframes:
        raise ValueError("No data was successfully fetched")
    
    final_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"\nTotal rows fetched: {len(final_df)}")
    
    return final_df
