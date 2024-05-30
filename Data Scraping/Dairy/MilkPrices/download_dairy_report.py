import requests
from bs4 import BeautifulSoup
import urllib.parse
import os

def download_latest_dairy_report():
    base_url = "https://www.ers.usda.gov"
    dairy_data_url = urllib.parse.urljoin(base_url, "/data-products/dairy-data/dairy-data/")
    
    # Send a GET request to the dairy data page
    response = requests.get(dairy_data_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table row containing the report link and date
        report_link = None
        report_date = None
        
        table_rows = soup.find_all('tr')
        for row in table_rows:
            link_tag = row.find('a', href=True, string="U.S. Dairy situation at a glance (monthly and annual)")
            if link_tag:
                report_link = urllib.parse.urljoin(base_url, link_tag['href'])
                date_cell = row.find_all('td', class_='text-center')[0]
                if date_cell:
                    report_date = date_cell.text.strip()
                break
        
        if report_link and report_date:
            report_filename = f"USDA_Dairy_Report_{report_date.replace('/', '-')}.xlsx"
            if os.path.exists(report_filename):
                print("Report is up to date.")
            else:
                # Download the report
                print(f"Downloading report from {report_link}")
                report_response = requests.get(report_link)
                
                # Save the report to a file
                if report_response.status_code == 200:
                    with open(report_filename, 'wb') as f:
                        f.write(report_response.content)
                    print("Report downloaded successfully.")
                else:
                    print(f"Failed to download report. Status code: {report_response.status_code}")
        else:
            print("Report link or date not found.")
    else:
        print(f"Failed to retrieve dairy data page. Status code: {response.status_code}")

# Run the function to download the latest dairy report
download_latest_dairy_report()
