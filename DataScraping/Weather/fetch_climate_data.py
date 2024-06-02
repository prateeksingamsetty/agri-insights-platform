#This code was written to fetch the past 5 years weather data i.e., from Jan 1st, 2019 to May 31st, 2024. If anytime you would like to fetch hourly
#between any specific date range, just change the start and end dates in the url. But you need to generate this your own url from the website
# https://api.climate.ncsu.edu/

#Below are the instructions on how you will be able to fet your own url
#Step 1: First create an account in the above given website
#Step 2: Generate an API Hash key for yourself used in order for authenticate your API request.
#Step 3: Under the My API tab, select My API builder
#Step 4: Now, choose the specific date range, location, and the variables you want for your data
#Step 5: At the botton of your page, click "Show Generated URL and copy paste the url generated"
#Step 6: Finally, you can use your generated url in this code and run the file. Make sure to create a data.json file for the data to be
#appended to that file

import urllib.request
import json

url = "https://api.climate.ncsu.edu/data?loc=FLET&var=airtempavg,rhavg2m,precip1m,leafwetness,wbgtavg2m&start=2024-01-01%2000:00&end=2024-05-31%2023:59&int=1%20HOUR&obtype=H&output=json&attr=location,datetime,var,value,unit,paramtype,obnum,obavail,obtime,obmin,obmax,pdstart,pdend&hash=faae25b731f480f916263f954ef7e3ea9ee5897d"

def fetch_and_append():
    print("Called")

    with urllib.request.urlopen(url) as response:
        climate_json_string = response.read()

    # Check if response is empty
    if not climate_json_string:
        print("Error: Empty response from API")
        return

    try:
        climate_data_json = json.loads(climate_json_string)
        climate_data_json = climate_data_json["data"]["FLET"]

        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(climate_data_json, f, ensure_ascii=False, indent=4)

    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return
    
    print("Done")

fetch_and_append()