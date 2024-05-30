# Dairy Data Class Prices

This Python script fetches class dairy pricing data from the USDA Agricultural Marketing Service API and saves the data to JSON, Excel, and MongoDB.

## Description

The script performs the following tasks:

1. **Fetch Data from API**: Sends a GET request to the USDA API endpoint for dairy pricing data.
2. **Save Data to JSON**: Saves the fetched data to a JSON file.
3. **Save Data to MongoDB**: (Currently commented out) Inserts the fetched data into a MongoDB database.
4. **Save Data to Excel**: Saves the fetched data to an Excel file.
5. **Error Handling**: Provides basic error handling to manage exceptions during the data fetching and saving processes.

## Usage

To run the script:

1. Ensure you have Python installed.
2. Execute the script `dairy_data_class_prices.py`.
3. Check the generated files `dairy_data.json` (JSON file) and `dairy_data.xlsx` (Excel file) for the saved data.

## Dependencies

- Python 3.x
- `requests` library
- `pandas` library
- `pymongo` library (for MongoDB operations, if uncommented)

You can install the required libraries using pip
