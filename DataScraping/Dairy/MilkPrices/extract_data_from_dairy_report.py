import pandas as pd
from pymongo import MongoClient
import json

# Function to connect to MongoDB
def mongo_connect():
    client = MongoClient('mongodb://localhost:27017/')
    return client['dairy_database']  # Name of the database

# Function to read and process Excel file
def read_process_excel(file_path):
    # Load the Excel file
    data = pd.read_excel(file_path, skiprows=1)
    
    # Extract the specific row for 'All milk (at average milk-fat test)'
    milk_data = data[data['Unnamed: 1'].str.contains('All milk \(at average milk-fat test\)', na=False, regex=True)]
    if milk_data.empty:
        print("No data found for 'All milk (at average milk-fat test)'.")
        return pd.DataFrame()  # Return empty DataFrame if no data found
    
    # Prepare column names list for melting
    date_columns = [col for col in data.columns if 'Unnamed:' in col or 'Annual' in col]  # Adjust this as necessary
    
    # Melt the DataFrame with correct identification of date columns
    milk_data = milk_data.melt(id_vars=['Unnamed: 1', '  Units'], value_vars=date_columns, var_name='Date', value_name='Price')
    
    # Clean up the DataFrame
    milk_data.dropna(subset=['Price'], inplace=True)
    milk_data.rename(columns={'  Units': 'Unit'}, inplace=True)
    
    return milk_data

# Convert DataFrame to structured JSON
def format_json(data):
    json_data = {'All milk (at average milk-fat test)': {
        'Annual': {},
        'Monthly': {},
        'Units': data['Unit'].iloc[0] if not data.empty else '$/cwt'
    }}
    
    for _, row in data.iterrows():
        # Attempt to parse the date, if it fails, continue
        try:
            date_str = row['Date'].replace('Unnamed:', '').strip()
            date = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(date):
                key = date.strftime('%Y') if date.month == 1 else date.strftime('%d-%m-%Y')
                if date.month == 1:
                    json_data['All milk (at average milk-fat test)']['Annual'][key] = row['Price']
                else:
                    json_data['All milk (at average milk-fat test)']['Monthly'][key] = row['Price']
        except Exception as e:
            print("Error parsing date:", e)
            continue
    
    return json_data

# Store data in MongoDB
def store_in_mongodb(db, json_data):
    collection = db['milk_prices']
    collection.insert_one(json_data)

def main():
    db = mongo_connect()
    data = read_process_excel('USDA_Dairy_Report_5-16-2024.xlsx')  # Update the path
    if not data.empty:
        json_data = format_json(data)
        print(json_data)
        # store_in_mongodb(db, json_data)
        print("Data successfully stored in MongoDB and JSON format.")
    else:
        print("No data available to process and store.")

if __name__ == '__main__':
    main()
