import os
import pandas as pd
import matplotlib.pyplot as plt


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

def calculate_latency_throughput(file_path):
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
    # print(f"📏 Latency Summary:\n{df['latency'].describe()}")
    print(f"📏 Average Latency: {df['latency'].mean():.4f} ms")
    print(f"⚡ Throughput: {throughput:.4f} windows per second")

    _cleanup_file(file_path)

    return df  # Return DataFrame for further analysis if needed

def compare_files(files, show=False):
    plt.figure(figsize=(12, 6))  # Define figure size
    postfix = 'K/ms'

    average_latencies = []
    throughputs = []
    file_labels = []

    for file in files:
        try:
            file_name = os.path.basename(file).replace(".csv", "")
            # Read the CSV file without headers (assuming the last two columns contain timestamps)
            df = pd.read_csv(file, header=1, dtype=str)

            # Extract the last two columns (assuming they are start_time and end_time)
            start_time = pd.to_numeric(df.iloc[:, -2], errors="coerce")  # 2nd last column
            end_time   = pd.to_numeric(df.iloc[:, -1], errors="coerce")  # Last column

            # Calculate latency (end_time - start_time)
            latency = end_time - start_time

            # Plot the latency values
            plt.plot(latency.index, latency, label=f'{file_name}')  # Use file name as label

            # Compute and store Average Latency
            avg_latency = latency.mean()
            average_latencies.append(avg_latency)

            # Compute and store Throughput (total records / total time span)
            total_records = len(df)
            total_time_span = end_time.max() - start_time.min()
            throughput = total_records / (total_time_span / 1000) if total_time_span > 0 else 0  # Convert to seconds
            throughputs.append(throughput)

            # Store file name for bar chart labels
            file_labels.append(f'{file_name}')
            print(f"📊 {file_name:>15}: Avg Latency: {avg_latency:>6.4f} ms, Throughput: {throughput:>6.4f} windows/sec")
            # Store Average Latency and Throughput in a CSV file
            results_file = f"comparisn_.csv"
            results_data = {
                "File": [file_name],
                "Average Latency (ms)": [avg_latency],
                "Throughput (windows/sec)": [throughput]
            }
            results_df = pd.DataFrame(results_data)

            # Append to the results file or create it if it doesn't exist
            if not os.path.exists(results_file):
                results_df.to_csv(results_file, index=False)
            else:
                results_df.to_csv(results_file, mode="a", header=False, index=False)

        except Exception as e:
            print(f"❌ Error processing {file_name}: {e}")

    # ✅ Line Graph: Latency Trends
    plt.xlabel("Window")
    plt.ylabel("Latency (ms)")
    plt.title("Latency Trend Across Multiple Files")
    plt.legend(loc="upper right")  # Legend for different files
    plt.grid(True)

    # Show the latency trend plot
    if show: plt.show()

    # ✅ Bar Graph: Average Latency & Throughput
    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Plot Average Latency (Left Y-axis)
    ax1.set_xlabel("ingestion rate (K/ms)")
    ax1.set_ylabel("Average Latency (ms/window)", color='b')
    ax1.bar(file_labels, average_latencies, color='b', alpha=0.6, label="Avg Latency")
    ax1.tick_params(axis='y', labelcolor='b')

    # Create second Y-axis for Throughput
    ax2 = ax1.twinx()
    ax2.set_ylabel("Throughput (windows/sec)", color='r')
    ax2.plot(file_labels, throughputs, marker='o', linestyle='-', color='r', label="Throughput", linewidth=2)
    ax2.tick_params(axis='y', labelcolor='r')

    # Title and Grid
    plt.title("Average Latency & Throughput per File")
    fig.tight_layout()
    plt.grid(True)

    # Show the bar graph
    if show: plt.show()

def process_baseline_file(input_file, output_dir, chunk_size):
    """
    Reads a large CSV file in chunks, extracts specific values, and stores them in a new file.
    
    Extracted values:
    - First and last value of the first column
    - First value of the second-last column
    - Last value of the last column
    
    :param input_file: Path to the input CSV file.
    :param output_file: Path to the output CSV file.
    :param chunk_size: Number of rows per chunk.
    """

    first_chunk = True  # To handle writing the header in the output file
    output_file = os.path.join(output_dir, f"baseline_processed.csv")

    # Read CSV in chunks
    with pd.read_csv(input_file, chunksize=chunk_size, dtype=str) as reader:
        for chunk in reader:
            if len(chunk) != chunk_size:
                continue  # Skip last chunk if it's smaller than chunk_size
            
            # Extract required values
            first_col = chunk.iloc[:, 0]  # First column
            last_col = chunk.iloc[:, -1]  # Last column
            second_last_col = chunk.iloc[:, -2]  # Second-last column

            new_data = {
                "first": first_col.iloc[0],  # First value of first column
                "last": first_col.iloc[-1],  # Last value of first column
                "start": second_last_col.iloc[0],  # First value of second-last column
                "end": last_col.iloc[-1],  # Last value of last column
            }

            # Convert to DataFrame
            new_df = pd.DataFrame([new_data])

            # Append to output file
            new_df.to_csv(output_file, mode="a", header=first_chunk, index=False)
            first_chunk = False  # Ensure header is written only once

    print(f"✅ Processing complete! Extracted data saved at: {output_file}")
