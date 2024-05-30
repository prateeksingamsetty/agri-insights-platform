import urllib.request
import json
import pandas as pd
import csv
import time
import os

url = "https://api.climate.ncsu.edu/data?loc=FLET&var=airtempavg,precip1m,rhavg2m,wbgtavg2m,leafwetness&start=2023-12-20%2000:00&end=2024-05-20%2023:59&int=1%20HOUR&obtype=H&output=json&attr=location,datetime,var,value,value_accum,unit,score,paramtype,obtype,obnum,obavail,obtime,obmin,obmax,pdstart,pdend&hash=faae25b731f480f916263f954ef7e3ea9ee5897d"

filename = 'climateDataLoaded.csv'
headers = ['datetime','Average Relative Humidity', 'Average Air Temperature', 'Total Precipitation', 'Top-of-the-Hour Experimental Leaf Wetness', 'Average Wet Bulb Globe Temperature', 'location', 'obtype']

def read_last_timestamp():
    try:
        with open("last_timestamp.txt", "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return ""

def write_last_timestamp(timestamp):
    with open("last_timestamp.txt", "w") as file:
        file.write(timestamp)

def fetch_and_append():
    print("Called")
    last_timestamp = read_last_timestamp()
    # Check if the file is empty
    file_empty = os.stat(filename).st_size == 0

    with urllib.request.urlopen(url) as response:
        climate_json_string = response.read()

    # Check if response is empty
    if not climate_json_string:
        print("Error: Empty response from API")
        return

    try:
        climate_json_dict_object = json.loads(climate_json_string)
        climate_data_keys_list = list(climate_json_dict_object["data"]["FLET"].keys())
        data_rows = []

        #Fetch data from each object inside data->FLET (fetching all variables data, for different date records)
        for key in climate_data_keys_list:
            if not last_timestamp or key > last_timestamp:
                datetime = key
                location = climate_json_dict_object["data"]["FLET"][key]["rhavg2m"]["location"]
                obtype = climate_json_dict_object["data"]["FLET"][key]["rhavg2m"]["obtype"]
                avg_rel_humidity = climate_json_dict_object["data"]["FLET"][key]["rhavg2m"]["value"]
                avg_air_temp = climate_json_dict_object["data"]["FLET"][key]["airtempavg"]["value"]
                tot_prec = climate_json_dict_object["data"]["FLET"][key]["precip1m"]["value"]
                leaf_wetness = climate_json_dict_object["data"]["FLET"][key]["leafwetness"]["value"]
                avg_wet_bulb_globe_temp = climate_json_dict_object["data"]["FLET"][key]["wbgtavg2m"]["value"]
                obj = {'datetime': datetime, 'Average Relative Humidity': avg_rel_humidity, 'Average Air Temperature': avg_air_temp, 'Total Precipitation': tot_prec, 'Top-of-the-Hour Experimental Leaf Wetness': leaf_wetness, 'Average Wet Bulb Globe Temperature': avg_wet_bulb_globe_temp, 'location': location, 'obtype': obtype}
                data_rows.append(obj)

        if len(data_rows) > 0:
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                
                # Write headers only if the file is empty
                if file_empty:
                    writer.writeheader()

                # Write data rows to the CSV file
                writer.writerows(data_rows)
            write_last_timestamp(climate_data_keys_list[-1])
        print("Doneee")
    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

while True:
    fetch_and_append()
    time.sleep(60) #Sleep for # minutes and then fetch and append again