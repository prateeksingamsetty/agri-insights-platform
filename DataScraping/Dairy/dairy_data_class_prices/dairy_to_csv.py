from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb+srv://agri_analyst:Password123@cluster0.bdxk2dg.mongodb.net/Agri_Insights')
db = client['Agri_Insights']

def month_to_number(month_name):
    month_names = {
        'Jan': 1, 'Feb': 2, 'Mar': 3,
        'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9,
        'Oct': 10, 'Nov': 11, 'Dec': 12,
        'January': 1, 'February': 2, 'March': 3,
        'April': 4, 'May': 5, 'June': 6,
        'July': 7, 'August': 8, 'September': 9,
        'October': 10, 'November': 11, 'December': 12
    }
    return month_names.get(month_name, None)

def number_to_month(month_number):
    month_names = {
        1: 'Jan', 2: 'Feb', 3: 'Mar',
        4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep',
        10: 'Oct', 11: 'Nov', 12: 'Dec'
    }
    return month_names.get(month_number, None)

# Fetch data from both collections
# dairy_prices = list(db.dairyprices.find())
# mailbox_prices = list(db.mailboxappalachianprices.find())
dairy_prices = list(db.DairyPrices.find())
mailbox_prices = list(db.Appalachian_States.find())

# Debug print to check mailbox data structure
print("Sample mailbox price document:")
if mailbox_prices:
    print(mailbox_prices[0])

output_data = []
for dairy_row in dairy_prices:
    try:
        milk_month = month_to_number(dairy_row['report_month'])
        milk_year = dairy_row['report_year']
        class_iii_whole_price = float(dairy_row['ClassIIIWhole'])
        class_iv_whole_price = float(dairy_row['ClassIVWhole'])

        # Find matching mailbox price
        matching_mailbox = next(
            (mb for mb in mailbox_prices 
             if mb['report_year'] == milk_year), 
            None
        )
        
        # Debug print for matching
        print(f"\nProcessing: Year={milk_year}, Month={milk_month}")
        print(f"Marketing Area: {dairy_row['MarketingArea']}")
        if matching_mailbox:
            print(f"Found matching mailbox data: {matching_mailbox['Reporting_Area']}")
        
        # Get mailbox price for the specific month
        mailbox_price = None
        if matching_mailbox:
            month_abbr = number_to_month(milk_month)
            if month_abbr and month_abbr in matching_mailbox:
                try:
                    price_str = matching_mailbox[month_abbr]
                    print(f"Found price string: {price_str} for month {month_abbr}")
                    if price_str and price_str.strip():
                        mailbox_price = float(price_str)
                except (ValueError, TypeError) as e:
                    print(f"Error converting price: {e}")
                    mailbox_price = None

        # Set quarter flags
        quarter_flags = [
            1 if 1 <= milk_month <= 3 else 0,
            1 if 4 <= milk_month <= 6 else 0,
            1 if 7 <= milk_month <= 9 else 0,
            1 if 10 <= milk_month <= 12 else 0
        ]

        output_data.append([
            milk_month, 
            milk_year, 
            mailbox_price, 
            class_iii_whole_price, 
            class_iv_whole_price
        ] + quarter_flags)
    except Exception as e:
        print(f"Error processing row: {dairy_row}")
        print(f"Error message: {str(e)}")
        continue

# Create DataFrame and save to CSV
output_df = pd.DataFrame(
    output_data, 
    columns=['Month', 'Year', 'Mailbox Price', 
             'Class III Whole Price', 'Class IV Whole Price',
             'Q1', 'Q2', 'Q3', 'Q4']
)

# Sort the DataFrame by Year and Month
output_df = output_df.sort_values(['Year', 'Month'])

# Save to CSV
output_df.to_csv('dairy_prices_output.csv', index=False)


# Print summary of data
print("\nData Summary:")
print(f"Total rows processed: {len(output_df)}")
print(f"Rows with mailbox prices: {output_df['Mailbox Price'].notna().sum()}")
print("\nSample of final data:")
print(output_df.head())