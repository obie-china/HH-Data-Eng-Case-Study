import pandas as pd
import requests
from io import StringIO

# Google Drive URLs for the datasets
patients_url = 'https://drive.google.com/file/d/1-ChQ1qSMeHdt4u1ALCrxRzXhF42_F_R6/view?usp=drive_link'
hospital_visits_url = 'https://drive.google.com/file/d/1-GMpBVPbJyFWxrxO9E3LM5gOD1joHqBu/view?usp=drive_link'
doctors_url = 'https://drive.google.com/file/d/1-MXken9pABaO5g1PrZIqFfc4VAIjAw2f/view?usp=drive_link'

def download_data(file_url):
    """
    This function ingests the csv data directly from google drive and returns a DataFrame
    """
    try:
        # Get file from Google Drive
        file_id = file_url.split('/')[-2]
        download_url = 'https://drive.google.com/uc?export=download&id=' + file_id

        response = requests.get(download_url)
        response.raise_for_status()

        # Read raw string, return pandas DataFrame
        csv_raw = StringIO(response.text)
        return pd.read_csv(csv_raw)
    
    # On error return an empty dataframe
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file from {file_url}: {e}")
        return pd.DataFrame() 
    except pd.errors.ParserError as e:
        print(f"Error parsing the CSV file from {file_url}: {e}")
        return pd.DataFrame()


try:
    # Load the data
    patients_df = download_data(patients_url)
    hospital_visits_df = download_data(hospital_visits_url)
    doctors_df = download_data(doctors_url)

    # If the function returns an empty dataframe, raise an error
    if patients_df.empty or hospital_visits_df.empty or doctors_df.empty:
        raise ValueError("1 or more datasets could not be loaded. Check the URLs and data.")

    # Join the Hospital visits data to the patients data on patient_id
    df = hospital_visits_df.merge(
        patients_df.drop('created_at', axis=1).rename(
            columns={'id': 'patient_id', 'name': 'patient_name', 'sex': 'patient_sex'}
        ),
        on='patient_id',
        how='left'
    )

    # Join the new dataset to the doctors data on doctor_id 
    df = df.merge(
        doctors_df.drop('created_at', axis=1).rename(columns={'id': 'doctor_id', 'name': 'doctor_name'}),
        on='doctor_id',
        how='left'
    )

    # Convert the created_at column to datetime
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

    # Save the result to a CSV file, notify user of success
    df.to_csv("fact_hospital_visits.csv", index=False)
    print("File has been saved")

# On error print error
except ValueError as e:
    print(f"ValueError: {e}")
except KeyError as e:
    print(f"KeyError: Missing column on join: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")