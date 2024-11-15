# import requests
# import json
# from datetime import datetime, timedelta

# # Function to fetch data from the API
# def fetch_milk_prices(api_url, params):
#     response = requests.get(api_url, params=params)
#     if response.status_code == 200:
#         return response.json()
#     else:
#         response.raise_for_status()

# # Function to filter results for Appalachian States
# def filter_appalachian_states(data):
#     return [
#         record for record in data['results']
#         if record['Reporting_Area'] == 'Appalachian States'
#     ]

# # Set up the API URL and parameters
# api_url = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/3357/Mailbox%20Milk%20Prices"

# # Calculate the date range for the last 5 years
# end_date = datetime.now()
# start_date = end_date - timedelta(days=5*365)

# # Prepare parameters to fetch data within the date range
# params = {
#     'lastDays': 5 * 365  # Parameter to fetch data for the last 5 years
# }

# # Fetch the data
# try:
#     data = fetch_milk_prices(api_url, params)
#     filtered_data = filter_appalachian_states(data)
#     # Save the filtered data to a JSON file
#     with open('appalachian_milk_prices.json', 'w') as json_file:
#         json.dump(filtered_data, json_file, indent=4)
#     print("Data has been successfully fetched and saved to appalachian_milk_prices.json")
# except Exception as e:
#     print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     fetch_milk_prices(api_url, params)
import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os
# API URL
API_URL = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/3357/Mailbox%20Milk%20Prices"

# MongoDB connection (replace with your MongoDB connection string)
MONGO_URI = "mongodb+srv://agri_analyst:Password123@cluster0.bdxk2dg.mongodb.net/Agri_Insights"
MONGO_DB = "Agri_Insights"
MONGO_COLLECTION = "mailboxappalachianprices"

# File paths
CSV_FILE = "appalachian_milk_prices.csv"
XLS_FILE = "appalachian_milk_prices.xlsx"

def fetch_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def process_data(data):
    results = data['results']
    appalachian_data = [entry for entry in results if entry['Reporting_Area'] == 'Appalachian States']
    return appalachian_data

def update_mongodb(data):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    for entry in data:
        query = {
            'report_month': entry['report_month'],
            'report_year': entry['report_year']
        }
        collection.update_one(query, {'$set': entry}, upsert=True)
    
    client.close()

def update_csv(data):
    df = pd.DataFrame(data)
    existing_df = pd.read_csv(CSV_FILE) if os.path.exists(CSV_FILE) else pd.DataFrame()
    updated_df = pd.concat([existing_df, df]).drop_duplicates(subset=['report_month', 'report_year'], keep='last')
    updated_df.to_csv(CSV_FILE, index=False)

def update_xls(data):
    df = pd.DataFrame(data)
    existing_df = pd.read_excel(XLS_FILE) if os.path.exists(XLS_FILE) else pd.DataFrame()
    updated_df = pd.concat([existing_df, df]).drop_duplicates(subset=['report_month', 'report_year'], keep='last')
    updated_df.to_excel(XLS_FILE, index=False)

def main():
    try:
        raw_data = fetch_data()
        processed_data = process_data(raw_data)
        update_mongodb(processed_data)
        update_csv(processed_data)
        update_xls(processed_data)
        print(f"Data updated successfully at {datetime.now()}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()