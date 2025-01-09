import glob
import os
import time
import platform
from datetime import date, timedelta

import certifi
import requests
import pandas as pd
import pymongo
import boto3
import json
from tempfile import mkdtemp
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
    NoSuchElementException,
)

# Constants
DOWNLOAD_PATH = "/tmp"  # Lambda's writable directory

# Fetch MongoDB credentials from AWS Secrets Manager
def get_secret():
    secret_name = "mongodb_credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]

        # Parse the secret into a dictionary and extract username and password
        secret_dict = json.loads(secret)
        mongodb_username = secret_dict.get("mongodb_username")
        mongodb_password = secret_dict.get("mongodb_password")

        return mongodb_username, mongodb_password

    except Exception as e:
        print(f"Error fetching secret: {e}")
        return None, None


mongodb_username, mongodb_password = get_secret()
if not mongodb_username or not mongodb_password:
    raise ValueError("MongoDB credentials not found")

# MongoDB connection string
# MONGODB_CONNECTION_STRING = (
#     f"mongodb+srv://{mongodb_username}:{mongodb_password}@cluster0.mongodb.net/"
# )
MONGODB_CONNECTION_STRING = f"mongodb://localhost:27017/"


# Functions
def wait_and_click(driver, locator):
    wait.until(EC.element_to_be_clickable(locator)).click()


def select_dropdown_option(main_div_id, option_to_be_selected):
    retries = 3
    while retries > 0:
        try:
            div_element = wait.until(EC.element_to_be_clickable((By.ID, main_div_id)))
            div_element.click()
            dropdown_options = wait.until(
                EC.visibility_of_all_elements_located(
                    (By.CSS_SELECTOR, f"#{main_div_id} .chosen-results li")
                )
            )
            for option in dropdown_options:
                if option.text == option_to_be_selected:
                    option.click()
                    return
            retries -= 1
        except (
            StaleElementReferenceException,
            ElementClickInterceptedException,
            ElementNotInteractableException,
            TimeoutException,
        ):
            retries -= 1
            time.sleep(2)


def select_multiple_dropdown_options(main_div_id, options_to_be_selected):
    options_to_select = list(options_to_be_selected.keys())
    selected_options = 0
    retries = 3

    while selected_options < len(options_to_select):
        try:
            div_element = wait.until(
                EC.element_to_be_clickable((By.ID, main_div_id))
            )
            div_element.click()
            dropdown_options = wait.until(
                EC.visibility_of_all_elements_located(
                    (By.CSS_SELECTOR, f"#{main_div_id} .chosen-results li")
                )
            )

            option_found = False
            for option in dropdown_options:
                if option.text in options_to_be_selected and options_to_be_selected[
                    option.text
                ]:
                    options_to_be_selected[option.text] = False
                    option.click()
                    selected_options += 1
                    option_found = True
                    time.sleep(1)
                    break

            if not option_found:
                for option_text in options_to_select:
                    if options_to_be_selected[option_text]:
                        print(f"Option '{option_text}' not found, skipping.")
                        options_to_be_selected[option_text] = False
                        selected_options += 1
                        break

            if selected_options == len(options_to_select):
                return

        except (
            StaleElementReferenceException,
            ElementClickInterceptedException,
            ElementNotInteractableException,
            TimeoutException,
        ):
            retries -= 1
            if retries == 0:
                raise
            time.sleep(2)
        except NoSuchElementException:
            print(f"Dropdown or options not found for ID '{main_div_id}', retrying...")
            retries -= 1
            if retries == 0:
                raise
            time.sleep(2)


def click_button_with_retry(button_locator, retries=3):
    while retries > 0:
        try:
            button = wait.until(EC.element_to_be_clickable(button_locator))
            button.click()
            return
        except (
            StaleElementReferenceException,
            ElementClickInterceptedException,
            ElementNotInteractableException,
            TimeoutException,
        ):
            retries -= 1
            if retries == 0:
                raise
            time.sleep(2)


def convert_csv_to_json(download_path):
    csv_files = glob.glob(os.path.join(download_path, "Shipping*.csv"))
    if not csv_files:
        print("No CSV files found in the specified directory.")
        return

    latest_csv_file = max(csv_files, key=os.path.getctime)
    print(latest_csv_file)

    df = pd.read_csv(latest_csv_file)
    json_data = df.to_dict(orient="records")
    return json_data


def push_data_to_mongodb(data, db_name, collection_name, connection_string):
    try:
        # client = pymongo.MongoClient(connection_string, tlsCAFile=certifi.where())
        client = pymongo.MongoClient(
            connection_string)
        db = client[db_name]
        collection = db[collection_name]
        collection.insert_many(data)
        print(f"Successfully inserted {len(data)} documents into MongoDB")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
    except pymongo.errors.InvalidName as e:
        print(f"Invalid database or collection name: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Main function to be triggered
def lambda_handler(event, context):
    options = Options()
    # options = webdriver.ChromeOptions()

    # Set the binary location for Chrome (from the Docker image)
    # options.binary_location = "/opt/chrome"
    options.binary_location = '/opt/chrome/chrome'

    prefs = {"download.default_directory": DOWNLOAD_PATH}
    # options.add_experimental_option("prefs", prefs)
    # options.add_argument("--headless")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless=new")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280x1696")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--no-zygote")
    options.add_argument(f"--user-data-dir={mkdtemp()}")
    options.add_argument(f"--data-path={mkdtemp()}")
    options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    options.add_argument("--remote-debugging-port=9222")

    # Define the Chrome driver service
    service = Service("/opt/chromedriver")

    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 20)

    try:
        response = requests.get("https://mymarketnews.ams.usda.gov/public_data", timeout=10)  # 10 seconds timeout
        print(f"Website status: {response.status_code}")
    except requests.exceptions.Timeout:
        print("The request timed out. The server took too long to respond.")
    except requests.exceptions.RequestException as e:
        print(f"Error reaching the website: {e}")

    try:
        driver.get("https://mymarketnews.ams.usda.gov/public_data")
        market_tab = wait.until(EC.element_to_be_clickable((By.ID, "market-tab")))
        market_tab.click()

        select_dropdown_option(
            "market_types_by_market_types_chosen", "Shipping Point"
        )
        select_dropdown_option("drill_types_chosen", "Location - State")
        select_dropdown_option("aggregate_by_market_types_section_2_chosen", "Weekly")

        today = date.today()
        seven_days_ago = today - timedelta(days=6)

        input_date_element = wait.until(
            EC.presence_of_element_located(
                (By.ID, "from-date-by-market-types-section-2")
            )
        )
        input_date_element.send_keys(seven_days_ago.strftime("%m/%d/%Y"))
        input_date_element.send_keys(Keys.RETURN)

        input_date_element = wait.until(
            EC.presence_of_element_located(
                (By.ID, "to-date-by-market-types-section-2")
            )
        )
        input_date_element.send_keys(today.strftime("%m/%d/%Y"))
        input_date_element.send_keys(Keys.RETURN)

        select_multiple_dropdown_options(
            "commodities_by_market_types_section_2_chosen", {"Tomatoes": True}
        )
        select_multiple_dropdown_options(
            "locations_by_market_types_section_2_chosen",
            {
                "Atlanta (Forest Park), Georgia": True,
                "Columbia, South Carolina": True,
                "Miami, Florida": True,
                "Orlando (Oviedo), Florida": True,
                "Raleigh, North Carolina": True,
            },
        )

        click_button_with_retry((By.ID, "btnGetDataMarketType"))
        click_button_with_retry((By.CSS_SELECTOR, ".btn-save-as-csv"))

        time.sleep(5)

        json_data = convert_csv_to_json(DOWNLOAD_PATH)
        push_data_to_mongodb(
            json_data, "Agri_Insights", "tomatoprices", MONGODB_CONNECTION_STRING
        )

    except Exception as e:
        print(f"Error: {e}")
        raise e
    finally:
        driver.quit()
