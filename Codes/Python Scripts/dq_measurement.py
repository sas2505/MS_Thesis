import numpy as np
import pandas as pd

BASE_DIRECTORY = "E:/Personal_Drive_Backup/My Important Files/Study/Uni-Bamberg/Thesis/Odysseus/Benchmarking/Dataset/Processed"
WINDOW_SIZE = 10000  # Number of rows per window
VOLATILITY = 3000  # Reference time for timeliness calculation

def _calculate_window_accuracy(data_window):
    """
    Calculate accuracy of a given window using MAD for incorrect value detection.
    
    :param data_window: List or NumPy array of values in the window
    :return: Accuracy score for the window, MAD value, V_T count, and Median
    """
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


def process_csv(file_path, column_name, window_size=WINDOW_SIZE):
    """
    Reads a large CSV file in chunks and computes accuracy over windows of data.

    :param file_path: Path to the CSV file
    :param column_name: Name of the column containing numerical data
    :param window_size: Number of rows per window (also used as chunk size)
    :return: DataFrame with accuracy per window, MAD, V_T values, and Median
    """
    print(f"🚀 Processing {file_path} in chunks of {window_size} rows...")

    accuracies = []
    mad_values = []
    vt_values = []
    median_values = []
    value_start = []
    value_end = []
    threshold_values = []
    completeness_values = []
    timeliness_values = []

    total_rows = 0  # Track number of rows processed
    
    # Read the CSV file in chunks of `window_size`
    with pd.read_csv(file_path, chunksize=window_size, dtype=str) as reader:
        for chunk in reader:
            if column_name not in chunk.columns:
                raise ValueError(f"❌ Column '{column_name}' not found in the CSV file.")

            # Convert to numeric (handles missing values)
            temp_data = pd.to_numeric(chunk[column_name], errors="coerce")
            # Process only full windows
            if len(chunk) == window_size:
                first_Value_id = chunk['value_id'].iloc[0]
                last_Value_id = chunk['value_id'].iloc[window_size - 1]


                # Calculate accuracy for each window
                window = temp_data.to_numpy()
                accuracy, mad, V_T, median, threshold = _calculate_window_accuracy(window)

                # Store results
                value_start.append(first_Value_id)
                value_end.append(last_Value_id)
                accuracies.append(accuracy)
                mad_values.append(mad)
                vt_values.append(V_T)
                median_values.append(median)
                threshold_values.append(threshold)

                print(
                    f"✅ Value_ID {first_Value_id:>10}-{last_Value_id:<10} | "
                    f"Median: {median:>11.6f} | MAD: {mad:>10.6f} | V_T: {V_T:>6} | "
                    f"Accuracy: {accuracy:>5.6f} | Threshold: {threshold:>11.6f}", end=' | '
                )

                # Calculate completeness for each window
                window = chunk.fillna("")  # Ensures empty values are represented as ""
                completeness, missing_count = _calculate_window_completeness(window)
                completeness_values.append(completeness)
                print(
                    f"Completeness: {completeness:>5.4f} | Missing Values: {missing_count}", end=' | '
                )


                # Calculate timeliness for each window
                timeliness = _calculate_window_timeliness(chunk, volatility=VOLATILITY)
                timeliness_values.append(timeliness)
                print(f"Timeliness: {timeliness:>5.4f}")
                print("-" * 60)
                total_rows += len(chunk)

    # Store results in a DataFrame
    result_df = pd.DataFrame({
        "Value_Start": value_start,
        "Value_End": value_end,
        "Accuracy": accuracies,
        # "Median": median_values,
        # "MAD": mad_values,
        # "V_T (Incorrect Values)": vt_values,
        # "Threshold": threshold_values,
        "Completeness": completeness_values,
        "Timeliness": timeliness_values
    })

    # result_df = result_df.iloc[::-1].reset_index(drop=True)  # Reverse order

    print(f"🎉 Processing complete! Total rows processed: {total_rows}")
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
    comparison_df = pd.read_csv(comparison_file, index_col=False, dtype=str)  # Read everything as string initially

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

    print(f"✅ Comparison complete! Differences calculated.")
    return diff_df



# Example usage
file_path = BASE_DIRECTORY + "/sensor_7125_final.csv"
result_df = process_csv(file_path, "value")
comparison_file = BASE_DIRECTORY + "/odysseus_result.csv"
difference_df = compare_results(result_df, comparison_file)

# Display the comparison DataFrame
print(difference_df)