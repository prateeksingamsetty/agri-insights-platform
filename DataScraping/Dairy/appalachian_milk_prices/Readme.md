# Appalachian Milk Prices

This Python script fetches milk price data for the Appalachian States from the USDA Agricultural Marketing Service API and saves the filtered results to a JSON file.

## Description

The script performs the following tasks:

1. **Fetch Data from API**: Sends a GET request to the USDA API endpoint for mailbox milk prices.
2. **Filter Appalachian States**: Filters the fetched data to include only records for the Appalachian States.
3. **Calculate Date Range**: Calculates the date range for the last 5 years to fetch relevant data.
4. **Save Filtered Data**: Saves the filtered data to a JSON file named `appalachian_milk_prices.json`.
5. **Error Handling**: Provides basic error handling to manage exceptions during the data fetching process.

## Usage

To run the script:

1. Ensure you have Python installed.
2. Execute the script `appalachian_milk_prices.py`.
3. Check the generated JSON file `appalachian_milk_prices.json` for the filtered data.

## Dependencies

- Python 3.x
- `requests` library

You can install the required library using pip
