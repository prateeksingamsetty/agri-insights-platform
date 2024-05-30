# Climate Data Fetcher

## **1. Overview**
This script fetches climate data from a specified API, processes it, and appends it to a CSV file. The script runs in a loop, fetching new data every minute and ensuring no duplicate entries by keeping track of the last processed timestamp.

## **2. Features**
- **Fetches hourly climate data** for specific variables such as air temperature, precipitation, humidity, and more.
- **Appends new data to a CSV file**.
- **Keeps track of the last processed timestamp** to avoid duplicate entries.
- **Runs continuously**, fetching and appending new data every minute.

## **3. Setup**

### **3.1. Clone the Repository**
```bash
git clone https://github.com/yourusername/climate-data-fetcher.git
cd climate-data-fetcher

```
### **3.2. Install Dependencies**
Ensure you have Python installed. No additional dependencies are required as the script uses standard libraries (`urllib`, `json`, `pandas`, `csv`, `time`, `os`).

### **3.3. Set Up Environment**
Create an empty `climateDataLoaded.csv` file in the directory:
Also, Create aan empty last_timestamp.txt file in the directory

## **4. Running the Script**
```bash
python ClimateDataApi.py file
