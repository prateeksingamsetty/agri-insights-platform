# Market News Data Scraper

This project automates the process of extracting market data from the USDA Market News website using Selenium WebDriver. The script selects specific options, fetches data for a given date range, and downloads the resulting CSV file.

## Prerequisites

1. **Python 3.x**
2. **Google Chrome**
3. **ChromeDriver**
4. Required Python package: **selenium**

### Installation

1. **Install Google Chrome:**
   Download and install the latest version of Google Chrome.

2. **Install ChromeDriver:**
   Download the ChromeDriver that matches your version of Chrome. Ensure it's added to your system PATH.

3. **Clone the Repository and Install Packages:**
   ```bash
   pip install selenium

## Script Description

The script performs the following steps:

1. **Initialize WebDriver:** Starts a new Chrome browser session.
2. **Open the USDA Market News Website:** Navigates to the public data page.
3. **Wait for Page Load:** Waits for the page elements to load.
4. **Select Dropdown Options:** Selects "Shipping Point," "Location - State," and "Weekly" options.
5. **Enter Date Range:** Inputs the start and end dates.
6. **Select Commodities and Locations:** Chooses "Tomatoes" and multiple locations.
7. **Fetch Data:** Clicks the "Get Data" button.
8. **Download CSV File:** Clicks the "Save as CSV" button.
9. **Wait and Close:** Waits for the download to complete, then closes the browser.
