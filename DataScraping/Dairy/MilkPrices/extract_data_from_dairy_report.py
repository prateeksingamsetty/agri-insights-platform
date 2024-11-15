import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import boto3
import io

# API URL
API_URL = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/3357/Mailbox%20Milk%20Prices"

# MongoDB connection (replace with your MongoDB connection string)
MONGO_URI = "mongodb://username:password@host:port/database"
MONGO_DB = "milk_prices_db"
MONGO_COLLECTION = "appalachian_states"

# S3 configuration
S3_BUCKET = "your-s3-bucket-name"
CSV_KEY = "appalachian_milk_prices.csv"
XLS_KEY = "appalachian_milk_prices.xlsx"

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

def update_s3_file(data, file_key, file_type):
    s3 = boto3.client('s3')
    
    # Check if file exists in S3
    try:
        existing_obj = s3.get_object(Bucket=S3_BUCKET, Key=file_key)
        existing_data = pd.read_csv(io.BytesIO(existing_obj['Body'].read())) if file_type == 'csv' else pd.read_excel(io.BytesIO(existing_obj['Body'].read()))
    except s3.exceptions.NoSuchKey:
        existing_data = pd.DataFrame()

    new_df = pd.DataFrame(data)
    updated_df = pd.concat([existing_data, new_df]).drop_duplicates(subset=['report_month', 'report_year'], keep='last')
    
    # Save to buffer
    buffer = io.BytesIO()
    if file_type == 'csv':
        updated_df.to_csv(buffer, index=False)
    else:
        updated_df.to_excel(buffer, index=False)
    buffer.seek(0)
    
    # Upload to S3
    s3.upload_fileobj(buffer, S3_BUCKET, file_key)

def main():
    try:
        raw_data = fetch_data()
        processed_data = process_data(raw_data)
        update_mongodb(processed_data)
        update_s3_file(processed_data, CSV_KEY, 'csv')
        update_s3_file(processed_data, XLS_KEY, 'xlsx')
        print(f"Data updated successfully at {datetime.now()}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()