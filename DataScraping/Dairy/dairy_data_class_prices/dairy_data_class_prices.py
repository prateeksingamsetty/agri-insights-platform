import requests
import json
import pandas as pd
from pymongo import MongoClient

# Function to fetch data from API
def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# Function to save data to a JSON file
def save_to_json(data, file_name):
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file, indent=4)

# Function to save data to MongoDB
def save_to_mongodb(data, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)

# Function to save data to an Excel file
def save_to_excel(data, file_name):
    df = pd.DataFrame(data)
    df.to_excel(file_name, index=False)

def main():
    api_url = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2991/detail"
    json_file_name = "dairy_data.json"
    excel_file_name = "dairy_data.xlsx"
    db_name = "dairy_db"
    collection_name = "prices"

    # Fetch data from API
    data = fetch_data(api_url)

    # Extract the 'results' section
    results = data.get("results", [])

    # Save the 'results' section to a JSON file
    save_to_json(results, json_file_name)

    # Save the 'results' section to MongoDB
    # save_to_mongodb(results, db_name, collection_name)

    # Save the 'results' section to an Excel file
    save_to_excel(results, excel_file_name)

    print("Data successfully saved to JSON file, Excel file, and MongoDB")

if __name__ == "__main__":
    main()
