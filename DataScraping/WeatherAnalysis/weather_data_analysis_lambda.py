from pymongo import MongoClient
import pandas as pd
import numpy as np  
from datetime import datetime, timedelta
import boto3
from io import StringIO

# MongoDB connection setup
client = MongoClient("mongodb+srv://agri_analyst:Password123@cluster0.bdxk2dg.mongodb.net/Agri_Insights")
db = client["Agri_Insights"]

def fetch_previous_consecutive_hours():
    """
    Fetch the last record from the previous day for continuity in calculations.

    Returns:
        dict: A dictionary containing the previous consecutive hours or None if not found.
    """
    # Calculate the timestamp for the previous day's last hour
    prev_date = (datetime.now() - timedelta(days=7)).replace(hour=23, minute=0, second=0, microsecond=0)
    # prev_date_str = prev_date.strftime('%Y-%m-%d %H:%M:%S')
    prev_date_str = '2024-12-30 23:00:00'

    # Query for the specific record
    collection = db["weather_analysis"]
    previous_consecutive_hours = collection.find_one(
        {"timestamp": prev_date_str},
        {
            "consecutive_hours": 1,  # Include this field
            "timestamp": 1          # Include timestamp field
        }
    )

    if previous_consecutive_hours:
        print(f"Previous consecutive hours record found for {prev_date_str}.")
        return {
            "timestamp": pd.to_datetime(previous_consecutive_hours["timestamp"]),
            "consecutive_hours": previous_consecutive_hours.get("consecutive_hours", 0)
        }
    else:
        print(f"No previous consecutive hours record found for {prev_date_str}.")
        return None

previous_consecutive_hours = fetch_previous_consecutive_hours()


def fetch_data_from_db():
    """
    Fetch data from MongoDB for the last 7 days, starting from 00:00:00 on the start day
    to 23:00:00 on the end day.

    Returns:
        pd.DataFrame: DataFrame containing the fetched records.
    """
    # Calculate start and end dates
    end_date = datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
    start_date = (end_date - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Convert to string format for MongoDB query
    # start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    # end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    start_date_str = '2024-12-31 00:00:00'  # Start date as Dec 31, 2024, 12:00 AM
    end_date_str = '2025-01-06 23:00:00'    # End date as Jan 06, 2025, 11:59 PM

    print(f"Fetching data from {start_date_str} to {end_date_str}...")

    # Collection name
    collection = db["weatherData"]

    # MongoDB query to fetch records within the range
    cursor = collection.find(
        {
            "timestamp": {"$gte": start_date_str, "$lte": end_date_str}
        },
        {
            "timestamp": 1,
            "measurements.rhavg2m.value": 1, 
            "measurements.airtempavg.value": 1, 
            "measurements.precip1m.value": 1
        }
    )

    data = []
    for doc in cursor:
        try:
            timestamp = pd.to_datetime(doc["timestamp"])
            
            # Extract and validate values
            rh_value = doc["measurements"]["rhavg2m"]["value"]
            rh_value = float(rh_value) if rh_value != "MV" else np.nan

            rainfall = doc["measurements"]["precip1m"]["value"]
            rainfall = float(rainfall) if rainfall != "MV" else np.nan

            temp_value = doc["measurements"]["airtempavg"]["value"]
            temp_value = float(temp_value) if temp_value != "MV" else np.nan
            
            # Append the parsed data
            data.append({
                "timestamp": timestamp, 
                "relative_humidity": rh_value, 
                "air_temperature": temp_value, 
                "rainfall": rainfall
            })
        except Exception as e:
            print(f"Error parsing document: {doc}")
            print(f"Error: {e}")
    
    print(f"Fetched {len(data)} entries successfully.")
    return pd.DataFrame(data)


def calculate_consecutive_hours(data, threshold, previous_consecutive_hours):
    """
    Calculate consecutive hours based on a threshold for relative humidity.

    Args:
        data (pd.DataFrame): The DataFrame containing the weather data.
        threshold (int or float): The relative humidity threshold to check against.
        previous_consecutive_hours (dict): The previous consecutive hours record.

    Returns:
        pd.DataFrame: Updated DataFrame with `consecutive_hours` calculated.
    """
    print(f"Calculating consecutive hours for RH >= {threshold}%...")
    print("previous_consecutive_hours ", previous_consecutive_hours)

    # Initialize 'consecutive_hours' column
    data["consecutive_hours"] = 0  # Default to 0 for all rows

    # Handle the first row explicitly
    if previous_consecutive_hours:
        if data.loc[0, "relative_humidity"] >= threshold:
            data.loc[0, "consecutive_hours"] = previous_consecutive_hours["consecutive_hours"] + 1
        else:
            data.loc[0, "consecutive_hours"] = 0
    else:
        # If no previous consecutive hours record, start from 0
        data.loc[0, "consecutive_hours"] = 0

    # Handle the remaining rows
    for i in range(1, len(data)):
        if data.loc[i, "relative_humidity"] >= threshold:
            data.loc[i, "consecutive_hours"] = data.loc[i - 1, "consecutive_hours"] + 1
        else:
            data.loc[i, "consecutive_hours"] = 0

    print(f"Consecutive hours calculated for {len(data)} rows.")
    return data


def calculate_blight_units(data):
    print("Calculating blight units based on consecutive hours, air temperature, and cultivar resistance...")
    
    def get_blight_units(temp_celsius, hours, resistance):
        # Logic based on Table 3 for the specific cultivar resistance
        if resistance == "S":  # Susceptible
            if temp_celsius > 27 and hours <= 24:
                return 0
            elif 23 <= temp_celsius <= 27:
                if 7 <= hours <= 9:
                    return 1
                elif 10 <= hours <= 12:
                    return 2
                elif 13 <= hours <= 15:
                    return 3
                elif 16 <= hours <= 18:
                    return 4
                elif 19 <= hours <= 24:
                    return 5
                elif hours<=6:
                    return 0
            elif 13 <= temp_celsius <= 22:
                if hours <= 6:
                    return 0
                elif 7<=hours<=9:
                    return 5
                elif 10<=hours<=12:
                    return 6
                elif 13 <= hours <= 24:
                    return 7
            elif 8 <= temp_celsius <= 12:
                if hours<=6:
                    return 0
                elif hours==7:
                    return 1
                elif 8<= hours <= 9:
                    return 2
                elif  hours==10:
                    return 3
                elif 11<= hours <= 12:
                    return 4
                elif 13 <= hours <=15:
                    return 5
                elif 16 <= hours <=24:
                    return 6
            elif 3 <= temp_celsius <= 7:
                if hours<=9:
                    return 0
                if 10 <= hours <= 12:
                    return 1
                elif 13 <= hours <= 15:
                    return 2
                elif 16 <= hours <= 18:
                    return 3
                elif 19 <= hours <= 24:
                    return 4
                
            elif temp_celsius < 3 and hours <= 24:
                return 0
            

        elif resistance == "MS":  # Moderately Susceptible
            if temp_celsius > 27 and hours <= 24:
                return 0
            elif 23 <= temp_celsius <= 27:
                if hours<=9:
                    return 0
                elif 10 <= hours <= 18:
                    return 1
                elif 19 <= hours <= 24:
                    return 2
            elif 13 <= temp_celsius <= 22:
                if hours <= 6:
                    return 0
                elif hours == 7:
                    return 1
                elif hours == 8:
                    return 2
                elif hours == 9:
                    return 3
                elif hours == 10:
                    return 4
                elif 11 <= hours <= 12:
                    return 5
                elif 13 <= hours <= 24:
                    return 6
            elif 8 <= temp_celsius <= 12:
                if hours <= 6:
                    return 0
                elif 7 <= hours <= 9:
                    return 1
                elif 10 <= hours <= 12:
                    return 2
                elif 13 <= hours <= 15:
                    return 3
                elif 16 <= hours <= 18:
                    return 4
                elif 19 <= hours <= 24:
                    return 5
            elif 3 <= temp_celsius <= 7:
                if hours <= 12:
                    return 0
                elif 13 <= hours <= 24:
                    return 1
            elif temp_celsius < 3 and hours <=24:
                return 0
            

        elif resistance == "MR":  # Moderately Resistant
            if temp_celsius > 27 and hours <= 24:
                return 0
            
            elif 23 <= temp_celsius <= 27:
                if hours <= 15:
                    return 0
                if 16 <= hours <= 24:
                    return 1
             
            elif 13 <= temp_celsius <= 22:
                if hours <= 6:
                    return 0
                elif hours == 7:
                    return 1
                elif hours == 8:
                    return 2
                elif hours == 9:
                    return 3
                elif 10 <= hours <= 12:
                    return 4
                elif 13 <= hours <= 24:
                    return 5
                
            elif 8 <= temp_celsius <= 12:
                if hours <= 9:
                    return 0
                
                elif 10 <= hours <= 12:
                    return 1
                elif 13 <= hours <= 15:
                    return 2
                elif 16 <= hours <= 24:
                    return 3
            elif 3 <= temp_celsius <= 7:
                if hours <= 18:
                    return 0
                elif 19 <= hours <= 24:
                    return 1
             
            elif temp_celsius < 3 and hours <= 24:
                return 0
        return 0  # Default if no condition matches

    # Add blight units for each resistance type
    for resistance in ["S", "MS", "MR"]:
        blight_units = []
        for _, row in data.iterrows():
            # Convert temperature from Fahrenheit to Celsius
            temp_celsius = (row["air_temperature"] - 32) * 5.0 / 9.0
            hours = row["consecutive_hours"]
            blight_units.append(get_blight_units(temp_celsius, hours, resistance))
        data[f"blight_units_{resistance}"] = blight_units

    print(f"Blight units calculated for {len(data)} rows for all cultivar resistances.")
    return data


def calculate_severity_values(data):
    print("Calculating severity values based on uninterrupted hours of RH >= 80% and temperature ranges...")

    def get_continuous_uninterrupted_hours_80(data):
        """Calculate continuous uninterrupted hours of RH >= 80%, considering 2-hour breaks."""
        uninterrupted_hours = [0] * len(data)
        for i in range(1, len(data)):
            if data.loc[i, "relative_humidity"] >= 80:
                uninterrupted_hours[i] = uninterrupted_hours[i - 1] + 1
            else:
                # Check if the break is <= 2 hours
                if i > 2 and data.loc[i - 1, "relative_humidity"] >= 80 and data.loc[i - 2, "relative_humidity"] >= 80:
                    uninterrupted_hours[i] = uninterrupted_hours[i - 1]  # Maintain the count
                else:
                    uninterrupted_hours[i] = 0
        return uninterrupted_hours

    def assign_severity_value(temp_fahrenheit, uninterrupted_hours):
        """Assign severity values based on temperature range and uninterrupted hours."""
        temp_celsius = (temp_fahrenheit - 32) * 5.0 / 9.0  # Convert temperature to Celsius
        if 45 <= temp_fahrenheit <= 53:  # Range [1]
            if uninterrupted_hours < 16:
                return 0
            elif 16 <= uninterrupted_hours <= 18:
                return 1
            elif 19 <= uninterrupted_hours <= 21:
                return 2
            elif 22 <= uninterrupted_hours <= 24:
                return 3
            elif 25 <= uninterrupted_hours <= 27:
                return 4
            elif 28 <= uninterrupted_hours <= 30:
                return 5
            elif 31 <= uninterrupted_hours <= 33:
                return 6
        elif 54 <= temp_fahrenheit <= 59:  # Range [2]
            if uninterrupted_hours < 13:
                return 0
            elif 13 <= uninterrupted_hours <= 15:
                return 1
            elif 16 <= uninterrupted_hours <= 18:
                return 2
            elif 19 <= uninterrupted_hours <= 21:
                return 3
            elif 22 <= uninterrupted_hours <= 24:
                return 4
            elif 25 <= uninterrupted_hours <= 27:
                return 5
            elif 28 <= uninterrupted_hours <= 30:
                return 6
        elif 60 <= temp_fahrenheit <= 80:  # Range [3]
            if uninterrupted_hours < 10:
                return 0
            elif 10 <= uninterrupted_hours <= 12:
                return 1
            elif 13 <= uninterrupted_hours <= 15:
                return 2
            elif 16 <= uninterrupted_hours <= 18:
                return 3
            elif 19 <= uninterrupted_hours <= 21:
                return 4
            elif 22 <= uninterrupted_hours <= 24:
                return 5
            elif 25 <= uninterrupted_hours <= 27:
                return 6
        return 0  # Default severity value

    # Calculate uninterrupted hours
    data["continuous_uninterrupted_hours_80"] = get_continuous_uninterrupted_hours_80(data)
    
    # Calculate severity values
    severity_values = []
    for _, row in data.iterrows():
        temp = row["air_temperature"]
        hours = row["continuous_uninterrupted_hours_80"]
        severity_values.append(assign_severity_value(temp, hours))
    data["severity_values"] = severity_values

    print(f"Severity values calculated for {len(data)} rows.")
    return data


def calculate_fungicide_units(data, days_since_application):
    """
    Calculate fungicide units based on daily rainfall and days since the last fungicide application.

    Parameters:
        data: DataFrame containing the daily rainfall amounts.
        days_since_application: User-provided days since the last fungicide application.
    """
    print(f"Calculating fungicide units for {len(data)} rows...")
    
    def get_fungicide_units(daily_rainfall, days_since_application):
        """Determine fungicide units based on rainfall and days since the last application."""
        if days_since_application == 1:
            if daily_rainfall < 1:
                return 1
        elif days_since_application == 2:
            if daily_rainfall < 1:
                return 1
            elif 2 <= daily_rainfall <= 4:
                return 4
            elif 5 <= daily_rainfall <= 8:
                return 5
            elif daily_rainfall > 8:
                return 6
        elif days_since_application == 3:
            if daily_rainfall < 1:
                return 1
            elif 1 <= daily_rainfall <= 2:
                return 2
            elif 3 <= daily_rainfall <= 5:
                return 4
            elif daily_rainfall > 5:
                return 5
        elif 4 <= days_since_application <= 5:
            if daily_rainfall < 1:
                return 1
            elif 1 <= daily_rainfall <= 3:
                return 3
            elif 4 <= daily_rainfall <= 8:
                return 4
            elif daily_rainfall > 8:
                return 5
        elif 6 <= days_since_application <= 9:
            if daily_rainfall < 1:
                return 1
            elif daily_rainfall > 4:
                return 4
        elif 10 <= days_since_application <= 14:
            if daily_rainfall < 1:
                return 0
            elif daily_rainfall == 1:
                return 1
            elif 2 <= daily_rainfall <= 4:
                return 2
            elif daily_rainfall > 4:
                return 5
        elif days_since_application > 14:
            if daily_rainfall < 1:
                return 0
            elif 1 <= daily_rainfall <= 8:
                return 1
            elif daily_rainfall > 8:
                return 2
        return 0  # Default value if no conditions are met

    # Add fungicide units to the DataFrame
    fungicide_units = []
    for _, row in data.iterrows():
        try:
            # Extract daily rainfall from the MongoDB data (assume in inches, convert to mm)
            daily_rainfall = row.get("daily_rainfall", 0) * 25.4  # Convert inches to mm
            fungicide_units.append(get_fungicide_units(daily_rainfall, days_since_application))
        except Exception as e:
            print(f"Error calculating fungicide units for row: {row}")
            print(f"Error: {e}")
            fungicide_units.append(None)

    data["fungicide_units"] = fungicide_units
    print("Fungicide units added to the data.")
    return data


def calculate_blitecast_severity_values(data):
    """
    Calculate Blitecast severity values based on average temperature and consecutive hours of RH >= 90%.

    Parameters:
        data: DataFrame containing air temperature and consecutive hours of RH >= 90%.
    """
    print("Calculating Blitecast severity values based on temperature and RH conditions...")

    def assign_blitecast_severity(temp_celsius, hours):
        """Determine severity value based on temperature range and hours of RH >= 90%."""
        if 7.2 <= temp_celsius <= 11.6:
            if hours < 15:
                return 0
            elif 15 <= hours <= 18:
                return 1
            elif 19 <= hours <= 21:
                return 2
            elif 22 <= hours <= 24:
                return 3
            elif hours >= 25:
                return 4
        elif 11.7 <= temp_celsius <= 15.0:
            if hours < 12:
                return 0
            elif 12 <= hours <= 15:
                return 1
            elif 16 <= hours <= 18:
                return 2
            elif 19 <= hours <= 21:
                return 3
            elif hours >= 22:
                return 4
        elif 15.1 <= temp_celsius <= 26.6:
            if hours < 9:
                return 0
            elif 9 <= hours <= 12:
                return 1
            elif 13 <= hours <= 15:
                return 2
            elif 16 <= hours <= 18:
                return 3
            elif hours >= 19:
                return 4
        return 0  # Default value if no condition matches

    # Ensure consecutive hours for RH >= 90% are calculated
    if "consecutive_hours" not in data.columns:
        data = calculate_consecutive_hours(data, 90, previous_consecutive_hours)

    # Calculate Blitecast severity values
    severity_values = []
    for _, row in data.iterrows():
        temp_celsius = (row["air_temperature"] - 32) * 5.0 / 9.0  # Convert Fahrenheit to Celsius
        hours = row["consecutive_hours"]
        severity_values.append(assign_blitecast_severity(temp_celsius, hours))
    
    # Add severity values to the DataFrame
    data["blitecast_severity"] = severity_values
    print("Blitecast severity values calculated and added to the data.")
    return data


def calculate_hutton_criteria(data):
    """
    Calculate Hutton Criteria based on consecutive days meeting the conditions:
    1. Minimum temperature of at least 10°C.
    2. At least 6 hours with RH >= 90%.

    Parameters:
        data: DataFrame containing air temperature and hours of RH >= 90%.
    """
    print("Calculating Hutton Criteria for consecutive days...")
    
    # Ensure consecutive hours for RH >= 90% are calculated
    if "consecutive_hours" not in data.columns:
        data = calculate_consecutive_hours(data, 90, previous_consecutive_hours)

    # Convert temperature from Fahrenheit to Celsius
    data["temp_celsius"] = (data["air_temperature"] - 32) * 5.0 / 9.0

    # Initialize Hutton Criteria column
    hutton_criteria = [0] * len(data)

    for i in range(1, len(data)):
        # Check conditions for the current day and the previous day
        if (
            data.loc[i, "temp_celsius"] >= 10 and
            data.loc[i - 1, "temp_celsius"] >= 10 and
            data.loc[i, "consecutive_hours"] >= 6 and
            data.loc[i - 1, "consecutive_hours"] >= 6
        ):
            hutton_criteria[i] = 1

    # Add Hutton Criteria to the DataFrame
    data["hutton_criteria"] = hutton_criteria
    print("Hutton Criteria calculated and added to the data.")
    return data

def push_final_data_to_db(data):
    """
    Push the processed DataFrame into the specified MongoDB collection.

    Parameters:
        data (pd.DataFrame): Processed data with all columns to be inserted as records.
    """
    collection_name = "weather_analysis"
    collection = db[collection_name]

    print(f"Pushing data to MongoDB collection '{collection_name}'...")

    # Convert DataFrame to a list of dictionaries (records)
    records = data.to_dict(orient="records")

    # Convert timestamp to the desired string format
    for record in records:
        if isinstance(record["timestamp"], pd.Timestamp):
            record["timestamp"] = record["timestamp"].strftime('%Y-%m-%d %H:%M:%S')

    try:
        # Insert all records into the collection
        collection.insert_many(records)
        print(f"Successfully pushed {len(records)} records to MongoDB.")
    except Exception as e:
        print("An error occurred while pushing data to MongoDB.")
        print(f"Error: {e}")

def upload_to_s3(bucket_name, file_name, data_frame):
    """
    Upload a DataFrame as a CSV file to an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file to store in the S3 bucket.
        data_frame (pd.DataFrame): DataFrame to upload.
    """
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    print(f"Data successfully uploaded to s3://{bucket_name}/{file_name}")

def fetch_from_s3(bucket_name, file_name):
    """
    Fetch an existing CSV file from an S3 bucket and load it into a DataFrame.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file in the S3 bucket.

    Returns:
        pd.DataFrame: DataFrame containing the data from the S3 file, or an empty DataFrame if the file doesn't exist.
    """
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successfully fetched data from s3://{bucket_name}/{file_name}")
            return pd.read_csv(response["Body"])
        else:
            print(f"Failed to fetch data from s3://{bucket_name}/{file_name}. Status: {status}")
            return pd.DataFrame()
    except s3.exceptions.NoSuchKey:
        print(f"File s3://{bucket_name}/{file_name} not found. Returning an empty DataFrame.")
        return pd.DataFrame()

def save_to_s3(bucket_name, file_name, new_data):
    """
    Merge new data with existing data in an S3 CSV file and update the file.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file in the S3 bucket.
        new_data (pd.DataFrame): DataFrame containing new data to merge.
    """
    # Fetch existing data from S3
    existing_data = fetch_from_s3(bucket_name, file_name)

    # Merge the new data with the existing data
    if not existing_data.empty:
        print("Merging new data with existing data...")
        updated_data = pd.concat([existing_data, new_data]).drop_duplicates(subset="timestamp").reset_index(drop=True)
    else:
        print("No existing data found. Using new data as the base.")
        updated_data = new_data

    # Drop any duplicate rows based on the 'timestamp' column
    updated_data = updated_data.drop_duplicates(subset="timestamp").reset_index(drop=True)

    # Upload the updated data back to S3
    upload_to_s3(bucket_name, file_name, updated_data)

def main():
      # Configure Pandas to display all rows and columns
    pd.set_option('display.max_rows', 100)  # Adjust to show all rows
    pd.set_option('display.max_columns', None)  # Adjust to show all columns (optional)

    print("Starting the script...")
    
    raw_data = fetch_data_from_db()
    print("Data fetched from MongoDB.")
    print(raw_data)  # Display the entire DataFrame

    # Calculate consecutive hours for RH >= 90%
    data = calculate_consecutive_hours(raw_data, 90, previous_consecutive_hours)
    print("Consecutive hours calculated.")
    
    # Calculate blight units
    data = calculate_blight_units(data)
    print("Blight units calculated.")
    
    # Calculate severity values for RH >= 80%
    data = calculate_severity_values(data)
    print("Severity values calculated.")
    
    # Calculate fungicide units
    days_since_application = 3  # You can modify this or make it user input
    data = calculate_fungicide_units(data, days_since_application)
    print("Fungicide units calculated.")
    
    # Calculate Blitecast severity values
    data = calculate_blitecast_severity_values(data)
    print("Blitecast severity values calculated.")
    
    # Calculate Hutton Criteria
    data = calculate_hutton_criteria(data)
    print("Hutton Criteria calculated.")

    #Push the data into MongoDB Database
    push_final_data_to_db(data)
    
    #Save the data to csv file in our S3 bucket
    bucket_name = "my-weather-analysis-bucket" 
    file_name = "comprehensive_potato_blight_analysis.csv"

    # Save to S3 (fetch existing data, merge, and upload the updated data)
    save_to_s3(bucket_name, file_name, data)

    # Reset Pandas display options to default (optional)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')

if __name__ == "__main__":
    main()