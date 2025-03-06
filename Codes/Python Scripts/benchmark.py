import pandas as pd
import os

FILE_PATH = "E:/Personal_Drive_Backup/My Important Files/Study/Uni-Bamberg/Thesis/Odysseus/Benchmarking/Dataset/Processed/odysseus_result.csv"

def _fix_file(file_path):
    """
    Since the CSV  file produced by Odysseus has missing header names for TimeInterval timestamps,
    this method fixes the CSV file by adding missing headers.
    """
    output_file = file_path.replace(".csv", "_fixed.csv")
    # Read the original file
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Modify the first line by appending missing headers
    lines[0] = lines[0].strip() + ",start_time,end_time\n"

    # Write the modified content to a new file
    with open(output_file, "w", encoding="utf-8") as f:
        f.writelines(lines)

def _cleanup_file(file_path):
    """
    Deletes the temporary file created during the file fixing process.
    """
    cleanup_file = file_path.replace(".csv", "_fixed.csv")
    os.remove(cleanup_file)

def calculate_latency_throughput(file_path, save_output=True):
    """
    Reads a CSV file and calculates latency & throughput.
    
    :param file_path: Path to the CSV file
    :param save_output: Save the processed results to a new CSV file (default: True)
    """
    # Fix the file by adding missing headers
    _fix_file(file_path)
    fixed_file_path = file_path.replace(".csv", "_fixed.csv")

    # Read CSV without headers 
    df = pd.read_csv(fixed_file_path, dtype=str)  

    # Convert start_time and end_time to numeric 
    df["start_time"] = pd.to_numeric(df["start_time"], errors="coerce")  
    df["end_time"] = pd.to_numeric(df["end_time"], errors="coerce")

    # Calculate Latency (End Time - Start Time)
    df["latency"] = df["end_time"] - df["start_time"]

    # Calculate Throughput: Total windows / Total time span
    total_records = len(df)
    total_time_span = df["end_time"].max() - df["start_time"].min()
    throughput = total_records / (total_time_span / 1000) if total_time_span > 0 else 0

    # Print Results
    print(f"✅ Total Records Processed: {total_records}")
    print(f"📏 Latency Summary:\n{df['latency'].describe()}")
    print(f"⚡ Throughput: {throughput:.4f} windows per second")

    # Save processed results (optional)
    if save_output:
        output_file = file_path.replace(".csv", "_latency_throughput.csv")
        df.to_csv(output_file, index=False)
        print(f"📁 Processed file saved as: {output_file}")

    _cleanup_file(file_path)

    return df  # Return DataFrame for further analysis if needed

# Example Usage:
df_result = calculate_latency_throughput(FILE_PATH, save_output=False)
