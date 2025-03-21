import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter


def calculate_statistics(file_path, chunk_size, column_name='value'):
    """
    Reads a CSV file in chunks, counts unique values in a specified column, and plots distribution.

    :param file_path: Path to the large CSV file.
    :param column_name: Name of the column to count unique values.
    :param chunk_size: Number of rows per chunk.
    :return: Dictionary of value counts.
    """
    count = 0
    value_counts = Counter()  # Dictionary to store counts of unique values
    max_value = float('-inf')
    min_value = float('inf')
    # Read the file in chunks
    with pd.read_csv(file_path, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            count += len(chunk)
            if column_name not in chunk.columns:
                raise ValueError(f"❌ Column '{column_name}' not found in the file.")
            
            # Count values in the chunk
            value_counts.update(chunk[column_name].value_counts().to_dict())
            chunk[column_name] = pd.to_numeric(chunk[column_name], errors='coerce')
            max_value = max(max_value, chunk[column_name].max())
            min_value = min(min_value, chunk[column_name].min())

    
    # Display statistics on the plot
    stats_text = (
        f"Total Rows: {count}\n"
        f"Unique Values: {len(value_counts)}\n"
        f"Max: {max_value}\n"
        f"Min: {min_value}\n"
    )
    
    
    # Convert to DataFrame for visualization
    df_counts = pd.DataFrame(value_counts.items(), columns=[column_name, "Count"]).sort_values(by="Count", ascending=False)

    # Plot the distribution
    no_of_columns = 20
    plt.figure(figsize=(12, 6))
    plt.bar(df_counts[column_name][:no_of_columns], df_counts["Count"][:no_of_columns], color='skyblue')  # Show top 20 values
    plt.text(0.95, 0.95, stats_text, transform=plt.gca().transAxes,
                fontsize=12, verticalalignment='top', horizontalalignment='right',
                bbox=dict(boxstyle="round,pad=0.3", edgecolor="black", facecolor="lightyellow"))
    plt.xlabel(column_name)
    plt.ylabel("Frequency")
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top {min(no_of_columns, len(df_counts))} Value Distribution")
    plt.grid(axis="y")
    plt.show()

    return len(value_counts)	


   