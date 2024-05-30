# README

## Overview

This folder contains two Python scripts designed to automate the download and processing of USDA dairy reports.

### 1. download_dairy_report.py

This script downloads the latest USDA dairy report from the ERS website.

- **Fetches the Dairy Data Page**: Sends an HTTP GET request to the dairy data page.
- **Parses the HTML Content**: Uses BeautifulSoup to find the link to the latest dairy report and its publication date.
- **Checks for Existing Report**: Verifies if a report with the same date already exists locally.
- **Downloads the Report**: If the report is new, it downloads the Excel file and saves it with a filename that includes the report date.

### 2. extract_data_from_dairy_report.py

This script processes the downloaded USDA dairy report to extract specific data and store it in MongoDB.

- **Connects to MongoDB**: Establishes a connection to a local MongoDB instance.
- **Reads and Processes the Excel File**: Reads the downloaded Excel file and extracts data related to "All milk (at average milk-fat test)".
- **Formats Data as JSON**: Converts the extracted data into a structured JSON format.
- **Stores Data in MongoDB**: Inserts the formatted JSON data into a MongoDB collection.
