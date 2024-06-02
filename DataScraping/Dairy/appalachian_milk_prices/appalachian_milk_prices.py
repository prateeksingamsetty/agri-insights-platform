import requests
import json
from datetime import datetime, timedelta

# Function to fetch data from the API
def fetch_milk_prices(api_url, params):
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# Function to filter results for Appalachian States
def filter_appalachian_states(data):
    return [
        record for record in data['results']
        if record['Reporting_Area'] == 'Appalachian States'
    ]

# Set up the API URL and parameters
api_url = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/3357/Mailbox%20Milk%20Prices"

# Calculate the date range for the last 5 years
end_date = datetime.now()
start_date = end_date - timedelta(days=5*365)

# Prepare parameters to fetch data within the date range
params = {
    'lastDays': 5 * 365  # Parameter to fetch data for the last 5 years
}

# Fetch the data
try:
    data = fetch_milk_prices(api_url, params)
    filtered_data = filter_appalachian_states(data)
    # Save the filtered data to a JSON file
    with open('appalachian_milk_prices.json', 'w') as json_file:
        json.dump(filtered_data, json_file, indent=4)
    print("Data has been successfully fetched and saved to appalachian_milk_prices.json")
except Exception as e:
    print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_milk_prices(api_url, params)
