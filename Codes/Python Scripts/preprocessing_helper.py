'''
This module provides helper functions for preprocess.py to preprocess the raw sensor data. 
    - split_sensors_by_file
    - 
'''
import os
import pandas as pd
import numpy as np
import math


BASE_DIRECTORY = "E:/Personal_Drive_Backup/My Important Files/Study/Uni-Bamberg/Thesis/Odysseus/Benchmarking/Dataset"
chunk_size = 50000

def _get_sensor_id_from_filename(filename):
    """
    Extracts the sensor ID from a given filename.
    
    :param filename: Name of the sensor file.
    :return: The sensor ID extracted from the filename.
    """
    return os.path.basename(filename)[7:11]


def split_sensors():
    """ Split original input datasets into separate files for each sensor """
    dataset_1 = BASE_DIRECTORY + "/Smart-Home/human_activity_raw_sensor_data/sensor_sample_int.csv"
    dataset_2 = BASE_DIRECTORY + "/Smart-Home/human_activity_raw_sensor_data/sensor_sample_float.csv"
    split_sensors_by_file(dataset_1)
    split_sensors_by_file(dataset_2)


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
    sensor_id = _get_sensor_id_from_filename(sensor_file) 
    new_filename = f"sensor_{sensor_id}_{no_of_days}_days.csv"
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


def convert_datetime_to_timestamp(input_file):
    """
    Converts the 'timestamp' column to Unix timestamp format.
    
    :param input_file: Path to the sensor CSV file.
    """
    print(f"🚀 Converting 'timestamp' to Unix timestamp in {input_file}...")

    # Define output file
    output_dir = BASE_DIRECTORY + "/Processed"
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist
    new_filename = f"sensor_{_get_sensor_id_from_filename(input_file)}_temp_1.csv"
    output_file = os.path.join(output_dir, new_filename)

    # Read the file in chunks to avoid memory issues
    first_chunk = True  # Track if it's the first chunk

    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            # Convert timestamp column to Unix timestamp
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce").astype(int) // 10**6

            # Append data with header only for the first chunk
            chunk.to_csv(output_file, mode="a", index=False, header=first_chunk)
            first_chunk = False  # Ensure subsequent chunks don't write the header

    print(f"✅ 'timestamp' column converted to Unix timestamp in {output_file}")


def add_inaccuracy(input_file, deviation=0.05, outlier_percentage=0.02, outlier_factor=3):
    """ 
    Adds inaccuracy to large sensor data files by introducing noise and outliers in chunks.
    
    :param input_file: Path to the sensor CSV file.
    :param deviation: The standard deviation of the Gaussian noise to add (default: 0.05).
    :param outlier_percentage: The percentage of outliers to introduce (default: 0.02).
    :param outlier_factor: The factor by which to multiply outliers (default: 3).
    :param chunk_size: Number of rows to process per chunk (default: 500,000).
    """
    def _process_chunk(data):
        """ Adds Gaussian noise and outliers while preserving original data type and precision. """
        data = data.astype(float)  # Ensure numeric processing
        
        # Detect if original data was int
        is_int = np.all(data % 1 == 0)  

        # Compute noise (ensuring no negative values)
        noise_sigma = np.maximum(data * deviation, 1e-6)
        noisy_data = data + np.random.normal(0, noise_sigma)

        # Introduce outliers
        num_outliers = int(outlier_percentage * len(noisy_data))
        if num_outliers > 0:
            outlier_indices = np.random.choice(noisy_data.index, size=num_outliers, replace=False)
            random_signs = np.random.choice([1, -1], size=num_outliers)
            noisy_data.loc[outlier_indices] *= (random_signs * outlier_factor)

        if is_int:
            return noisy_data.round().astype(int)  # Convert back to int if original was int
        else:
            # Preserve decimal places dynamically
            decimal_places = data.astype(str).str.split('.').str[-1].str.len().max()
            return noisy_data.round(decimal_places)  # Convert back to rounded float

    print(f"🚀 Introducing inaccuracy in {input_file} ...")

    column = 'value'

    # Define output file
    output_dir = os.path.join(BASE_DIRECTORY, "Processed")
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist
    new_filename = f"sensor_{_get_sensor_id_from_filename(input_file)}_temp_2.csv"
    output_file = os.path.join(output_dir, new_filename)

    # Process the file in chunks
    first_chunk = True  # To handle writing headers
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            chunk[column] = _process_chunk(chunk[column])
            # Append to output file (first chunk writes headers, others do not)
            chunk.to_csv(output_file, mode="a", index=False, header=first_chunk)
            first_chunk = False  # Ensure header is only written once

            print(f"✅ Processed {len(chunk)} rows...")

    print(f"🎉 Processing complete! Output saved at: {output_file}")


def add_missing_values(input_file, missing_percentage=0.05):
    """
    Introduces missing values in the 'value' column of sensor data files.
    :param input_file: Path to the sensor CSV file.
    :param missing_percentage: The percentage of missing values to introduce (default:  0.05).
    """
    print(f"🚀 Introducing {input_file} with {missing_percentage*100:.2f}% missing values...")
    # Define output file
    output_dir = BASE_DIRECTORY + "/Processed"
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist
    new_filename = f"sensor_{_get_sensor_id_from_filename(input_file)}_temp_3.csv"
    output_file = os.path.join(output_dir, new_filename)

    first_chunk = True  # To handle writing headers

    # Read file in chunks
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            # Calculate exact number of missing values for this chunk
            num_missing = int(missing_percentage * len(chunk))

            if num_missing > 0:
                # Select `num_missing` unique indices randomly
                missing_indices = np.random.choice(chunk.index, size=num_missing, replace=False)

                # Assign missing values
                chunk.loc[missing_indices, "value"] = ''

            # Append to output file (write header only for the first chunk)
            chunk.to_csv(output_file, mode="a", header=first_chunk, index=False)
            first_chunk = False  # Ensure header is only written once

            print(f"✅ Processed {len(chunk)} rows, introduced {num_missing} missing values.")


    print(f"🎉 Processing complete! Output saved at: {output_file}")


def add_time_of_availability(input_file, validity_period=5000, outdated_percentage=0.1):
    """
    Adds an 'available_time' column to a large sensor dataset, simulating data arrival time.
    
    :param input_file: Path to the input CSV file.
    :param validity_period: Time period within which data is expected to be valid.
    :param outdated_percentage: Percentage of records that will be marked as outdated.
    """
    print(f"🚀 Adding maximum validity_period={validity_period} milliseconds and {outdated_percentage:.2%} outdated values")

    # Define output file
    output_dir = os.path.join(BASE_DIRECTORY, "Processed")
    os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist
    new_filename = f"sensor_{_get_sensor_id_from_filename(input_file)}_final.csv"
    output_file = os.path.join(output_dir, new_filename)

    first_chunk = True  # Handle header writing

    # Read file in chunks
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            # Convert timestamp to integer
            chunk["timestamp"] = chunk["timestamp"].astype(int)

            # Generate available_time based on timestamp + a random offset within validity_period
            chunk["available_time"] = chunk["timestamp"] + np.random.randint(0, validity_period, size=len(chunk))

            # Introduce outdated records (available_time > timestamp + validity_period)
            num_outdated = int(len(chunk) * outdated_percentage)
            if num_outdated > 0:
                outdated_indices = np.random.choice(chunk.index, size=num_outdated, replace=False)
                chunk.loc[outdated_indices, "available_time"] += np.random.randint(validity_period, validity_period * 2, size=num_outdated)

            # Save the modified chunk
            chunk.to_csv(output_file, mode="a", index=False, header=first_chunk)
            first_chunk = False  # Ensure header is only written once

            print(f"✅ Processed {len(chunk)} rows, added {num_outdated} outdated records.")

    print(f"🎉 Processing complete! Output saved as {output_file}")


def clean_temp_files(sensor_id):
    """
    Deletes temporary files created during preprocessing.
    """
    file_list = [
        BASE_DIRECTORY+ f'/Processed/sensor_{sensor_id}_temp_1.csv',
        BASE_DIRECTORY+ f'/Processed/sensor_{sensor_id}_temp_2.csv',
        BASE_DIRECTORY+ f'/Processed/sensor_{sensor_id}_temp_3.csv'
    ]

    for file in file_list:
        if os.path.exists(file):
            os.remove(file)
            print(f"🗑️ Deleted: {file}")