import numpy as np
import pandas as pd
import matplotlib.pyplot as plt



def _calculate_rmse(ground_truth, real_values):
    """
    Calculate Root Mean Square Error (RMSE) for a given window of values.
    
    :param ground_truth: Array of ground truth values
    :param real_values: Array of observed values
    :return: RMSE for the window
    """
    if len(ground_truth) != len(real_values) or len(ground_truth) == 0:
        return None  # Ensure both arrays are of the same length and not empty

    # Convert to NumPy arrays
    ground_truth = np.array(ground_truth, dtype=np.float64)
    real_values = np.array(real_values, dtype=np.float64)

    # Remove NaN values while keeping index alignment
    mask = ~np.isnan(real_values)  # Keep only non-NaN values
    ground_truth = ground_truth[mask]
    real_values = real_values[mask]

    # Check if filtered arrays are empty (to avoid division by zero)
    if len(ground_truth) == 0:
        return None  # Return None if no valid data remains

    # Compute RMSE
    rmse = np.sqrt(np.mean((ground_truth - real_values) ** 2))
    return rmse


def _calculate_window_accuracy(data_window):
    """
    Calculate accuracy of a given window using MAD for incorrect value detection.
    
    :param data_window: List or NumPy array of values in the window
    :return: Accuracy score for the window, MAD value, V_T count, and Median
    """
    WINDOW_SIZE = len(data_window)
    if len(data_window) == 0:
        return 1.0, 0, 0, 0  # If the window is empty, assume full accuracy, MAD=0, V_T=0, Median=0

    V_T = np.sum(np.isnan(data_window))  # Missing values count as incorrect
    data_window = data_window[~np.isnan(data_window)]  # Remove NaN values
    # Compute Median
    median = np.median(data_window)

    # Compute Median Absolute Deviation (MAD)
    mad = np.median(np.abs(data_window - median))
    
    # Normalization constant for MAD
    alpha = 1 / 0.6745  # Approximate for normal distribution

    # MAD Threshold
    threshold = 3 * mad * alpha

    # Count incorrect values (V_T)
    V_T += np.sum(np.abs(data_window - median) > threshold)
    
    # Total tuples in the window (N_A)
    N_A = WINDOW_SIZE

    # Compute Accuracy
    accuracy = 1 - (V_T / N_A) if N_A > 0 else 1  # Avoid division by zero

    return accuracy, mad, V_T, median, threshold


def _calculate_window_completeness(data_window):
    """
    Calculate completeness of a given window by checking if 'value_id', 'sensor_id', or 'value' is empty.
    
    :param data_window: DataFrame window containing value_id, sensor_id, and value columns
    :return: Completeness score for the window, number of missing values
    """
    total_values = len(data_window)
    
    # Count rows where  'value' is an empty string ("")
    # missing_values = (data_window[["value"]].eq("")).any(axis=1).sum()
    missing_values = data_window["value"].eq("").sum()
    
    # Compute completeness
    completeness = 1 - (missing_values / total_values) if total_values > 0 else 1  # Avoid division by zero
    
    return completeness, missing_values


def _calculate_window_timeliness(data_window, volatility):
    """
    Calculate timeliness for a given window.
    
    :param data_window: DataFrame window containing 'timestamp' and 'available_time'
    :param volatility: The reference time (e.g., 95th percentile of currency)
    :return: Average timeliness for the window
    """
    data_window["available_time"] = pd.to_numeric(data_window["available_time"], errors="coerce")
    data_window["timestamp"] = pd.to_numeric(data_window["timestamp"], errors="coerce")

    if len(data_window) == 0:
        return 1  # If the window is empty, assume perfect timeliness
    
    # Ensure working with a copy to prevent warnings
    data_window = data_window.copy()
    
    # Compute currency (delay)
    data_window.loc[:, "currency"] = data_window["available_time"].astype(int) - data_window["timestamp"].astype(int)
    
    # Compute timeliness using the formula: max(1 - (currency / volatility), 0)
    data_window.loc[:, "timeliness"] = np.maximum(1 - (data_window["currency"] / volatility), 0)
    
    # Return the average timeliness for the window
    return data_window["timeliness"].mean()


def measure_dqs(file_path_real, window_size, volatility, SHOW=False):
    """
    Reads a large CSV file in chunks and computes accuracy over windows of data.

    :param file_path_real: Path to the CSV file containing real-world data
    :param file_path_gt: Path to the CSV file containing ground truth data
    :param column_name: Name of the column containing numerical data
    :param window_size: Number of rows per window (also used as chunk size)
    :return: DataFrame with accuracy per window, MAD, V_T values, and Median
    """
    print(f"🚀 Processing {file_path_real} in chunks of {window_size} rows...")

    accuracies = []
    mad_values = []
    vt_values = []
    median_values = []
    value_start = []
    value_end = []
    threshold_values = []
    completeness_values = []
    timeliness_values = []
    first_Value_id = ''
    last_Value_id = ''

    total_rows = 0  # Track number of rows processed

    def _process_accuracy(real_values):
        accuracy, mad, V_T, median, threshold = _calculate_window_accuracy(real_values)

        # Store results
        value_start.append(first_Value_id)
        value_end.append(last_Value_id)
        accuracies.append(accuracy)
        mad_values.append(mad)
        vt_values.append(V_T)
        median_values.append(median)
        threshold_values.append(threshold)
        
        if SHOW:
            print(f"Accuracy: {accuracy:>5.6f}", end=' | ')

    def _process_completeness(real_chunk):
        real_chunk = real_chunk.fillna("")  # Ensures empty values are represented as ""
        completeness, missing_count = _calculate_window_completeness(real_chunk)
        completeness_values.append(completeness)
        if SHOW:
            print(
                f"Completeness: {completeness:>5.4f}", end=' | '
            )
    
    def _process_timeliness(real_chunk):
        timeliness = _calculate_window_timeliness(real_chunk, volatility=volatility)
        timeliness_values.append(timeliness)
        if SHOW: print(f"Timeliness: {timeliness:>5.4f}",)


    # Read the CSV file in chunks
    with pd.read_csv(file_path_real, chunksize=window_size, dtype=str) as real_reader:

        for real_chunk in real_reader:
            # Convert to numeric (handles missing values)
            real_values = pd.to_numeric(real_chunk["value"], errors="coerce").to_numpy()

            # Process only full windows
            if len(real_chunk) == window_size:
                first_Value_id = real_chunk['value_id'].iloc[0]
                last_Value_id = real_chunk['value_id'].iloc[window_size - 1]
                if SHOW: print(f"✅ID {first_Value_id:>10}-{last_Value_id:<10}", end=" | ")

                # Calculate accuracy for each window
                _process_accuracy(real_values)

                # Calculate completeness for each window
                _process_completeness(real_chunk)

                # Calculate timeliness for each window
                _process_timeliness(real_chunk)

                if SHOW: print("-" * 90)
                total_rows += len(real_chunk)

        if SHOW:
            avg_accuracy = np.mean(accuracies) if accuracies else 0
            avg_completeness = np.mean(completeness_values) if completeness_values else 0
            avg_timeliness = np.mean(timeliness_values) if timeliness_values else 0
            print(f"Average Accuracy: {avg_accuracy:.4f} | Average Completeness: {avg_completeness:.4f} | Average Timeliness: {avg_timeliness:.4f}")

    # Store results in a DataFrame
    result_df = pd.DataFrame({
        "Value_Start": value_start,
        "Value_End": value_end,
        "Accuracy": accuracies,
        "Completeness": completeness_values,
        "Timeliness": timeliness_values,
    })

    return result_df


def compare_results(result_df, comparison_file):
    """
    Compares an existing result DataFrame with another CSV file containing similar results.
    
    :param result_df: DataFrame containing calculated results.
    :param comparison_file: Path to the CSV file to compare against.
    :return: DataFrame with differences in metrics.
    """
    # Reset index to avoid mismatches
    result_df = result_df.reset_index(drop=True)

    # Load the comparison file, ensuring the first column isn't taken as an index
    comparison_df = pd.read_csv(
        comparison_file, 
        index_col=False, 
        skiprows=1,
        header=None,
        dtype=str)  # Read everything as string initially

    # Manually map result_df columns to the correct columns in comparison_df
    column_mapping = {
        "Accuracy": 0,       # 1st column in comparison file
        "Completeness": 1,   # 2nd column
        "Value_Start": 2,    # 3rd column
        "Value_End": 3,      # 4th column
        "Timeliness": 4      # 5th column
    }

    # Reorder `comparison_df` based on the correct column mapping
    comparison_df = comparison_df.iloc[:, [column_mapping["Value_Start"], 
                                           column_mapping["Value_End"], 
                                           column_mapping["Accuracy"], 
                                           column_mapping["Completeness"], 
                                           column_mapping["Timeliness"]]]

    # Rename columns in `comparison_df` to match `result_df`
    comparison_df.columns = result_df.columns

    # Convert all numeric columns to float in BOTH DataFrames
    for col in result_df.columns:
        result_df[col] = pd.to_numeric(result_df[col], errors='coerce')  # Ensure result_df is numeric
        comparison_df[col] = pd.to_numeric(comparison_df[col], errors='coerce')  # Ensure comparison_df is numeric

    # Compute differences
    diff_df = result_df - comparison_df

    # Filter rows where any column has an absolute value greater than 0.009
    diff_df = diff_df[(diff_df.abs() > 0.009).any(axis=1)]
    if len(diff_df) == 0:
        print("✅ Data Quality measurements are accurate upto two decimal points.")
    else:
        print("⚠️ Found differences in Data Quality measurements:")
        print(diff_df)
