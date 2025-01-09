"""
This code was written to fetch the past 5 years weather data i.e., from Jan 1st, 2019 to May 31st, 2024. If anytime you would 
like to fetch hourly between any specific date range, just change the start and end dates in the url. But you need to generate 
this your own url from the website https://api.climate.ncsu.edu/

Below are the instructions on how you will be able to fet your own url
Step 1: First create an account in the above given website
Step 2: Generate an API Hash key for yourself used in order for authenticate your API request.
Step 3: Under the My API tab, select My API builder
Step 4: Now, choose the specific date range, location, and the variables you want for your data
Step 5: At the botton of your page, click "Show Generated URL and copy paste the url generated"
Step 6: Finally, you can use your generated url in this code and run the file. Make sure to create a data.json file for the 
data to be appended to that file
"""

import urllib.request
import json
from datetime import date, timedelta
import certifi
import pymongo
import os

mongodb_username = os.getenv("MONGODB_USERNAME")
mongodb_password = os.getenv("MONGODB_PASSWORD")

# Calculate the date range for the past 7 days
today = date.today()
seven_days_ago = today - timedelta(days=6)

today_str = today.strftime("%Y-%m-%d")
seven_days_ago_str = seven_days_ago.strftime("%Y-%m-%d")

# Construct the URL for the API request
url = (
    f"https://api.climate.ncsu.edu/data?loc=FLET&var=airtempavg,rhavg2m,precip1m,leafwetness,wbgtavg2m"
    f"&start={seven_days_ago_str}%2000:00&end={
        today_str}%2023:59&int=1%20HOUR&obtype=H&output=json"
    f"&attr=location,datetime,var,value,unit,paramtype,obnum,obavail,obtime,obmin,obmax,pdstart,pdend"
    f"&hash=faae25b731f480f916263f954ef7e3ea9ee5897d"
)


def fetch_and_append():
    """
    Fetch data from the API and append it to the MongoDB collection.
    """
    print("Fetching data from API...")

    try:
        # Fetch data from the API
        with urllib.request.urlopen(url) as response:
            climate_json_string = response.read()
    except urllib.error.URLError as e:
        print(f"Error fetching data from API: {e}")
        return

    # Check if the response is empty
    if not climate_json_string:
        print("Error: Empty response from API")
        return

    try:
        climate_data = json.loads(climate_json_string)
        climate_data = climate_data["data"]["FLET"]
    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return
    except KeyError as e:
        print(f"Error accessing data in JSON: {e}")
        return

    try:
        # Connect to MongoDB
        connection_string = f"mongodb+srv://{mongodb_username}:{
            mongodb_password}@cluster0.bdxk2dg.mongodb.net/"
        client = pymongo.MongoClient(
            connection_string, tlsCAFile=certifi.where())
        db = client['Agri_Insights']
        collection = db['weatherData']

        # Prepare documents for MongoDB
        all_documents = []
        for timestamp, measurements in climate_data.items():
            if isinstance(measurements, dict):
                document = {
                    "timestamp": timestamp,
                    "measurements": measurements
                }
                all_documents.append(document)
            else:
                print(f"Unexpected data format for timestamp: {timestamp}")

        # Insert documents into the collection
        if all_documents:
            collection.insert_many(all_documents)
            print(f"Inserted {len(all_documents)} documents into MongoDB")
        else:
            print("No documents to insert")

    except pymongo.errors.PyMongoError as e:
        print(f"Error interacting with MongoDB: {e}")
    finally:
        # Ensure the MongoDB connection is closed
        client.close()


if __name__ == "__main__":
    fetch_and_append()
