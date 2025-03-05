'''
This module provides helper functions for preprocess.py to preprocess the raw sensor data. 
    - split_sensors_by_file
    - 
'''
import os
import pandas as pd

BASE_DIRECTORY = "E:/Personal_Drive_Backup/My Important Files/Study/Uni-Bamberg/Thesis/Odysseus/Benchmarking/Dataset"
chunk_size = 50000


def split_sensors_by_file(input_file):
    """
    Reads a large dataset in chunks and splits data for each sensor into separate files.
    
    :param input_file: Path to the CSV file containing sensor data.
    """
    print(f"🚀 Splitting {input_file} into sensor-specific files...")
    # Define output directory
    output_dir = BASE_DIRECTORY + "/Processed/individual_sensors"
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist

    # Dictionary to store open file handles for each sensor
    sensor_files = {}

    # Read dataset in chunks
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:  # Read everything as string
        for chunk in reader:
            # Group data by 'sensor_id'
            grouped = chunk.groupby("sensor_id")

            for sensor_id, sensor_data in grouped:
                sensor_filename = os.path.join(output_dir, f"sensor_{sensor_id}.csv")

                # Append data to respective sensor file
                mode = "w" if sensor_id not in sensor_files else "a"
                header = mode == "w"  # Write header only for the first time

                sensor_data.to_csv(sensor_filename, mode=mode, index=False, header=header)

                # Keep track of written sensor files
                sensor_files[sensor_id] = True

    print(f"✅ Data successfully split into sensor-specific files in: {output_dir}")



def extract_first_no_of_days(sensor_file, no_of_days):
    """
    Extracts the first N days of data from a sensor file and saves to a new file.
    
    :param sensor_file: Path to the sensor CSV file.
    """
    print(f"🚀Extracting first {no_of_days} days from {sensor_file}...")
    # Define output file
    output_dir = BASE_DIRECTORY + "/Processed"
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist
    original_filename = os.path.basename(sensor_file) 
    sensor_id = original_filename.replace(".csv", "")  
    new_filename = f"{sensor_id}_{no_of_days}_days.csv"
    output_file = os.path.join(output_dir, new_filename)

    # Initialize variables
    first_timestamp = None
    extracted_data = []
    TIMESTAMP_COLUMN = "timestamp"

    # Read the file in chunks to avoid memory issues
    with pd.read_csv(sensor_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            # Convert timestamp column to datetime with correct unit
            chunk[TIMESTAMP_COLUMN] = pd.to_datetime(chunk[TIMESTAMP_COLUMN], errors="coerce")  

            # If this is the first chunk, get the first timestamp
            if first_timestamp is None:
                first_timestamp = chunk[TIMESTAMP_COLUMN].iloc[0]  # Get first row timestamp

            # Calculate the cutoff timestamp (first_timestamp + N_DAYS)
            cutoff_time = first_timestamp + pd.Timedelta(days=no_of_days)

            # Filter rows within the first N days
            filtered_chunk = chunk[chunk[TIMESTAMP_COLUMN] < cutoff_time]

            # Append data
            extracted_data.append(filtered_chunk)

            # If we've processed all N days, stop early for efficiency
            if chunk[TIMESTAMP_COLUMN].iloc[-1] >= cutoff_time:
                break


    # Combine all extracted chunks and save
    if extracted_data:
        final_df = pd.concat(extracted_data)
        final_df.to_csv(output_file, index=False)
        print(f"✅ Extracted first {no_of_days} days from {os.path.basename(sensor_file)} → {output_file}")


def check_consistency(sensor_file):
    """
    Checks if the 'value_id' and 'timestamp' columns are strictly increasing.
    
    :param sensor_file: Path to the sensor CSV file.
    """
    print(f"🚀 Checking consistency in {sensor_file}")

    last_value_id = None
    last_timestamp = None
    inconsistent_rows = 0
    total_rows_checked = 0
    VALUE_ID_COLUMN = "value_id"
    TIMESTAMP_COLUMN = "timestamp"

    # Read the file in chunks for memory efficiency
    with pd.read_csv(sensor_file, chunksize=chunk_size, dtype=str, parse_dates=[TIMESTAMP_COLUMN]) as reader:
        for chunk in reader:
            # Convert value_id to numeric (handling missing or corrupted values)
            chunk[VALUE_ID_COLUMN] = pd.to_numeric(chunk[VALUE_ID_COLUMN], errors="coerce")
            
            # Convert timestamp to datetime
            chunk[TIMESTAMP_COLUMN] = pd.to_datetime(chunk[TIMESTAMP_COLUMN], errors="coerce", format="%Y-%m-%d %H:%M:%S.%f")

            # Iterate over rows in chunk
            for i, row in chunk.iterrows():
                total_rows_checked += 1
                
                # Check value_id consistency
                if last_value_id is not None and row[VALUE_ID_COLUMN] <= last_value_id:
                    inconsistent_rows += 1
                    print(f"❌ Inconsistent value_id at row {total_rows_checked}: {row[VALUE_ID_COLUMN]} (Previous: {last_value_id})")

                # Check timestamp consistency
                if last_timestamp is not None and row[TIMESTAMP_COLUMN] <= last_timestamp:
                    inconsistent_rows += 1
                    print(f"❌ Inconsistent timestamp at row {total_rows_checked}: {row[TIMESTAMP_COLUMN]} (Previous: {last_timestamp})")

                # Update last values
                last_value_id = row[VALUE_ID_COLUMN]
                last_timestamp = row[TIMESTAMP_COLUMN]

    if inconsistent_rows == 0:
        print(f"✅ {os.path.basename(sensor_file)} is **fully consistent** ✅")
    else:
        print(f"⚠️ {os.path.basename(sensor_file)} has {inconsistent_rows} inconsistencies ⚠️")
