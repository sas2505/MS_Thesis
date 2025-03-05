'''
This module processes the original dataset.
'''
import preprocessing_helper as ph
from preprocessing_helper import BASE_DIRECTORY



def split_sensors():
    """ Split original input datasets into separate files for each sensor """
    dataset_1 = BASE_DIRECTORY + "/Smart-Home/human_activity_raw_sensor_data/sensor_sample_int.csv"
    dataset_2 = BASE_DIRECTORY + "/Smart-Home/human_activity_raw_sensor_data/sensor_sample_float.csv"
    ph.split_sensors_by_file(dataset_1)
    ph.split_sensors_by_file(dataset_2)

def extract_first_n_days(sensor_id, no_of_days):
    """
    Extracts the first N days of data from a sensor file and saves to a new file.
    """
    ph.extract_first_no_of_days(
        sensor_file = BASE_DIRECTORY+ f'/Processed/individual_sensors/sensor_{sensor_id}.csv', 
        no_of_days = no_of_days
    )


# Step 1: Split original input datasets into separate files for each sensor
# split_sensors()
    
# Step 2: Extract the first N days of data from a sensor file
# extract_first_n_days(sensor_id="6223", no_of_days=3)
    
ph.check_consistency(sensor_file = BASE_DIRECTORY+ f'/Processed/sensor_6223_3_days.csv')