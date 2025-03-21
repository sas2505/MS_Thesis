'''
This module provides helper functions to preprocess the raw sensor data. 
'''
import os
import pandas as pd
import numpy as np


def split_sensors_by_file(input_file, output_dir, chunk_size):
    """
    Reads a large dataset in chunks and splits data for each sensor into separate files.
    
    :param input_file: Path to the CSV file containing sensor data.
    :param output_dir: Directory to save the sensor-specific files.
    :param chunk_size: Number of rows to process per chunk.
    """
    print(f"🚀 Splitting {os.path.basename(input_file)} into sensor-specific files...")
    
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


def extract_first_no_of_days(sensor_file, no_of_days, output_dir, chunk_size):
    """
    Extracts the first N days of data from a sensor file and saves to a new file.
    
    :param sensor_file: Path to the sensor CSV file.
    :param no_of_days: Number of days to extract.
    :param output_file: Path to save the extracted data.
    :param chunk_size: Number of rows to process per chunk.
    """
    input_file_name = os.path.basename(sensor_file).rsplit('.', 1)[0]
    print(f"⏳ Extracting first {no_of_days} days data from {input_file_name}.csv ...")
    # Define output file
    output_file = os.path.join(output_dir, f"{input_file_name}_original.csv")

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
        print(f"✅ Extracted first {no_of_days} days to {output_file}")


def convert_datetime_to_timestamp(input_file, chunk_size):
    """
    Converts the 'timestamp' column to Unix timestamp format.
    
    :param input_file: Path to the sensor CSV file.
    :param chunk_size: Number of rows to process per chunk.
    """
    print(f"🚀 Converting 'timestamp' to Unix timestamp in {input_file}...")

    # Define output file
    input_file_name = os.path.basename(input_file).rsplit('.', 1)[0]
    output_file = os.path.join(os.getcwd(), f"{input_file_name}_temp_1.csv")

    # Read the file in chunks to avoid memory issues
    first_chunk = True  # Track if it's the first chunk

    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            # Convert timestamp column to Unix timestamp
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce").astype(int) // 10**6

            # Append data with header only for the first chunk
            chunk.to_csv(output_file, mode="a", index=False, header=first_chunk)
            first_chunk = False  # Ensure subsequent chunks don't write the header

    # print(f"✅ 'timestamp' column converted to Unix timestamp in {output_file}")
    return output_file


def add_inaccuracy(input_file, chunk_size, deviation=0.05, outlier_percentage=0.02, outlier_factor=3):
    """ 
    Adds inaccuracy to large sensor data files by introducing noise and outliers in chunks.
    
    :param input_file: Path to the sensor CSV file.
    :param deviation: The standard deviation of the Gaussian noise to add (default: 0.05).
    :param outlier_percentage: The percentage of outliers to introduce (default: 0.02).
    :param outlier_factor: The factor by which to multiply outliers (default: 3).
    :param chunk_size: Number of rows to process per chunk.
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
    input_file_name = os.path.basename(input_file).rsplit('.', 1)[0][:-7]
    print(f"🚀 Introducing inaccuracy in {input_file_name} with {deviation*100:.2f}% noise and {outlier_percentage*100:.2f}% outliers ...")

    column = 'value'

    # Define output file
    output_file = os.path.join(os.getcwd(), f"{input_file_name}_temp_2.csv")

    # Process the file in chunks
    first_chunk = True  # To handle writing headers
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            chunk[column] = _process_chunk(chunk[column])
            # Append to output file (first chunk writes headers, others do not)
            chunk.to_csv(output_file, mode="a", index=False, header=first_chunk)
            first_chunk = False  # Ensure header is only written once

            # print(f"✅ Processed {len(chunk)} rows...")

    # print(f"🎉 Processing complete! Output saved at: {output_file}")
    return output_file


def add_missing_values(input_file, chunk_size, missing_percentage=0.05):
    """
    Introduces missing values in the 'value' column of sensor data files.
    :param input_file: Path to the sensor CSV file.
    :param missing_percentage: The percentage of missing values to introduce (default:  0.05).
    :param chunk_size: Number of rows to process per chunk.
    """
    input_file_name = os.path.basename(input_file).rsplit('.', 1)[0][:-7]
    print(f"🚀 Introducing {input_file_name} with {missing_percentage*100:.2f}% missing values...")
    # Define output file
    output_file = os.path.join(os.getcwd(), f"{input_file_name}_temp_3.csv")

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

            # print(f"✅ Processed {len(chunk)} rows, introduced {num_missing} missing values.")


    # print(f"🎉 Processing complete! Output saved at: {output_file}")
    return output_file


def add_time_of_availability(input_file, output_dir, chunk_size, validity_period=5000, outdated_percentage=0.1): 
    """
    Adds an 'available_time' column to a large sensor dataset, simulating data arrival time.
    
    :param input_file: Path to the input CSV file.
    :param validity_period: Time period within which data is expected to be valid.
    :param outdated_percentage: Percentage of records that will be marked as outdated.
    :param chunk_size: Number of rows to process per chunk.
    """
    input_file_name = os.path.basename(input_file).rsplit('.', 1)[0][:-7]
    print(f"🚀 Adding maximum validity_period={validity_period} milliseconds and {outdated_percentage:.2%} outdated values")

    # Define output file
    if input_file_name.endswith("_original"):
        input_file_name = input_file_name[:-9]
    output_file = os.path.join(output_dir, f"{input_file_name}_processed.csv")

    first_chunk = True  # Handle header writing
    count = 0

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
            count += len(chunk)
            # print(f"✅ Processed {len(chunk)} rows, added {num_outdated} outdated records.")
    print(f"📊 Total Processed {count} rows")
    # print(f"🎉 Processing complete! Output saved as {output_file}")
    return output_file


def clean_temp_files(input_file):
    """
    Deletes temporary files created during preprocessing.
    """
    input_file_name = os.path.basename(input_file).rsplit('.', 1)[0]
    file_list = [
        f'{input_file_name}_temp_1.csv',
        f'{input_file_name}_temp_2.csv',
        f'{input_file_name}_temp_3.csv'
    ]

    for file in file_list:
        if os.path.exists(file):
            os.remove(file)
            # print(f"🗑️ Deleted: {file}")