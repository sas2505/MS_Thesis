'''
This module processes the original dataset.
'''
import preprocessing_helper as ph
from preprocessing_helper import BASE_DIRECTORY


# Step 1: Split original input datasets into separate files for each sensor
# ph.split_sensors()

SENSOR_ID= "5896"# "7125"
NO_OF_DAYS= 60 # no of days to extract from the original dataset
DAVIATION= 0.05 # percentage of inaccuracy to be added
OUTLIER_FACTOR= 2 # factor to determine the outlier threshold
OUTLIER_PERCENTAGE= 0.5 # percentage of outliers to be added
MISSING_PERCENTAGE= 0.2 # percentage of missing values to be added
VALIDITY_PERIOD= 5000   # time of availability in milliseconds
OUTDATED_PERCENTAGE= 0.2# percentage of outdated values to be added

# Step 2: Extract the first N days of data from a sensor file
ph.extract_first_no_of_days(
    sensor_file = BASE_DIRECTORY+ f'/Processed/individual_sensors/sensor_{SENSOR_ID}.csv', 
    no_of_days = NO_OF_DAYS
)

# Step 3: Convert datetime to timestamp
ph.convert_datetime_to_timestamp(input_file = BASE_DIRECTORY+ f'/Processed/sensor_{SENSOR_ID}_original.csv')

# Step 4: Add inaccuracy to simulate real-world data
ph.add_inaccuracy(
    input_file = BASE_DIRECTORY+ f'/Processed/sensor_{SENSOR_ID}_temp_1.csv',
    deviation= DAVIATION,
    outlier_factor= OUTLIER_FACTOR,
    outlier_percentage= OUTLIER_PERCENTAGE
)

# Step 5: Add missing values to simulate real-world data
ph.add_missing_values(
    input_file = BASE_DIRECTORY+ f'/Processed/sensor_{SENSOR_ID}_temp_2.csv',
    missing_percentage= MISSING_PERCENTAGE
)

# Step 6: Add time of availability to simulate real-world data
ph.add_time_of_availability(
    input_file = BASE_DIRECTORY+ f'/Processed/sensor_{SENSOR_ID}_temp_3.csv',
    validity_period= VALIDITY_PERIOD,
    outdated_percentage= OUTDATED_PERCENTAGE
)

# Step 7: Delete temporary files
ph.clean_temp_files(sensor_id=SENSOR_ID)
