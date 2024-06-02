import urllib.request
import json
from datetime import date, timedelta
import pymongo

# Calculate the date range for the past 7 days
today = date.today()
seven_days_ago = today - timedelta(days=7)

today_str = today.strftime("%Y-%m-%d")
seven_days_ago_str = seven_days_ago.strftime("%Y-%m-%d")

# Construct the URL for the API request
url = (
    f"https://api.climate.ncsu.edu/data?loc=FLET&var=airtempavg,rhavg2m,precip1m,leafwetness,wbgtavg2m"
    f"&start={seven_days_ago_str}%2000:00&end={today_str}%2023:59&int=1%20HOUR&obtype=H&output=json"
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
        client = pymongo.MongoClient('mongodb://192.168.0.106:27017/')
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

    print("Done")

if __name__ == "__main__":
    fetch_and_append()