import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
from preprocessing_helper import extract_first_no_of_days, _get_sensor_id_from_filename

BASE_DIRECTORY = "E:/Personal_Drive_Backup/My Important Files/Study/Uni-Bamberg/Thesis/Odysseus/Benchmarking/Dataset"
chunk_size = 50000

def count_tuples(input_file):  
    """
    Returns the number of tuples in the given sensor CSV file.
    :param input_file: Path to the sensor CSV file.
    """
    print(f"📊 Counting tuples in sensor: {_get_sensor_id_from_filename(input_file)}", end=' ')
    count = 0
    # Read file in chunks
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            count += len(chunk)
    print(f"found: {count}")
    return count

def count_unique_values(file_path, column_name='value', chunk_size=chunk_size):
    """
    Reads a CSV file in chunks, counts unique values in a specified column, and plots distribution.

    :param file_path: Path to the large CSV file.
    :param column_name: Name of the column to count unique values.
    :param chunk_size: Number of rows per chunk.
    :return: Dictionary of value counts.
    """
    value_counts = Counter()  # Dictionary to store counts of unique values
    print(f"Processing : {_get_sensor_id_from_filename(file_path)}")
    # Read the file in chunks
    with pd.read_csv(file_path, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            if column_name not in chunk.columns:
                raise ValueError(f"❌ Column '{column_name}' not found in the file.")
            
            # Count values in the chunk
            value_counts.update(chunk[column_name].value_counts().to_dict())

    # Convert to DataFrame for visualization
    df_counts = pd.DataFrame(value_counts.items(), columns=[column_name, "Count"]).sort_values(by="Count", ascending=False)

    # Plot the distribution
    no_of_columns = 50
    plt.figure(figsize=(12, 6))
    plt.bar(df_counts[column_name][:no_of_columns], df_counts["Count"][:no_of_columns], color='skyblue')  # Show top 20 values
    plt.xlabel(column_name)
    plt.ylabel("Frequency")
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top {min(no_of_columns, len(df_counts))} {_get_sensor_id_from_filename(file_path)} Value Distribution")
    plt.grid(axis="y")
    plt.show()

    return value_counts


# SENSOR_ID = "6223"
# FILE_PATH = f"{BASE_DIRECTORY}/Processed/individual_sensors/sensor_{SENSOR_ID}.csv"
# count_tuples(FILE_PATH)

# sensor_list = [5894,5895,7125,5896,6127,6220,
#                6253,6632,6633,6634,6635,6636,
#                6896,6686,6687,7139,6222,6223,
#                5887,5888,5889,5891,5892,5893]
# for sensor in sensor_list:
#     FILE_PATH = f"{BASE_DIRECTORY}/Processed/sensor_{sensor}_original.csv"
#     # extract_first_no_of_days(FILE_PATH, 60)
#     count_tuples(FILE_PATH)

sensor_list = [5896,6127,6896,5892]
for sensor in sensor_list:
    FILE_PATH = f"{BASE_DIRECTORY}/Processed/sensor_{sensor}_original.csv"
    count_unique_values(FILE_PATH, column_name='value', chunk_size=chunk_size)
