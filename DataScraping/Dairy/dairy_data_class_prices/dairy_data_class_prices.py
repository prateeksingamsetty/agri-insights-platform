import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

def fetch_data(api_url):
    """Fetch data from the API"""
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {str(e)}")
        raise

def filter_data(results, marketing_area):
    """Filter data for specific marketing area"""
    try:
        filtered_data = [item for item in results if item['MarketingArea'] == marketing_area]
        print(f"Filtered {len(filtered_data)} records for {marketing_area}")
        return filtered_data
    except Exception as e:
        print(f"Error filtering data: {str(e)}")
        raise

def get_latest_entry(collection):
    """Get the latest entry from MongoDB"""
    try:
        return collection.find_one(
            sort=[('report_year', -1), ('report_month', -1)]
        )
    except Exception as e:
        print(f"Error getting latest entry from MongoDB: {str(e)}")
        raise

def save_to_mongodb(data, db_name, collection_name, mongo_uri):
    """Save new data to MongoDB"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        latest_entry = get_latest_entry(collection)
        
        new_entries = []
        for entry in data:
            if not latest_entry or (
                int(entry['report_year']) > int(latest_entry['report_year']) or 
                (int(entry['report_year']) == int(latest_entry['report_year']) and 
                 entry['report_month'] > latest_entry['report_month'])
            ):
                new_entries.append(entry)
        
        if new_entries:
            collection.insert_many(new_entries)
            print(f"Added {len(new_entries)} new entries to MongoDB")
        
        client.close()
        return new_entries
    
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")
        raise

def update_data():
    """Main function to update data"""
    print(f"\nStarting data update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configuration
    api_url = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/3355/Final%20Class%20Prices%20by%20Order"
    marketing_area = "Appalachian (Charlotte)"
    mongo_uri = "mongodb+srv://agri_analyst:Password123@cluster0.bdxk2dg.mongodb.net/Agri_Insights"
    
    try:
        # Fetch and filter data
        print("Fetching data from API...")
        data = fetch_data(api_url)
        filtered_results = filter_data(data.get("results", []), marketing_area)
        
        if not filtered_results:
            print("No data received from API")
            return
        
        # Save to MongoDB and get new entries
        new_entries = save_to_mongodb(
            filtered_results, 
            "Agri_Insights", 
            "DairyPrices", 
            mongo_uri
        )
            
    except Exception as e:
        print(f"Error in update_data: {str(e)}")
    finally:
        print(f"Data update process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        update_data()
    except Exception as e:
        print(f"Critical error in main: {str(e)}")