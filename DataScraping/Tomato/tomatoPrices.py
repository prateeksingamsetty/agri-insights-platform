import glob
import os
import time
import platform
from datetime import date, timedelta

import certifi
import pandas as pd
import pymongo
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
    NoSuchElementException
)

# Constants
if platform.system() == 'Windows':
    DOWNLOAD_PATH = "C:\\Users\\hp\\Downloads"
elif platform.system() == 'Linux':
    DOWNLOAD_PATH = "/home/runner/downloads"
else:
    raise OSError("Unsupported operating system.")

mongodb_username = os.getenv("MONGODB_USERNAME")
mongodb_password = os.getenv("MONGODB_PASSWORD")

MONGODB_CONNECTION_STRING = f"mongodb+srv://{mongodb_username}:{
    mongodb_password}@cluster0.bdxk2dg.mongodb.net/"

# Functions


def wait_and_click(driver, locator):
    """Wait for element to be clickable and click."""
    wait.until(EC.element_to_be_clickable(locator)).click()


def select_dropdown_option(main_div_id, option_to_be_selected):
    """Select option from dropdown."""
    retries = 3
    while retries > 0:
        try:
            div_element = wait.until(
                EC.element_to_be_clickable((By.ID, main_div_id)))
            div_element.click()
            dropdown_options = wait.until(EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, f"#{main_div_id} .chosen-results li")))
            for option in dropdown_options:
                if option.text == option_to_be_selected:
                    option.click()
                    return
            retries -= 1
        except (StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException, TimeoutException):
            retries -= 1
            time.sleep(2)


# def select_multiple_dropdown_options(main_div_id, options_to_be_selected):
#     """Select multiple options from dropdown."""
#     options_to_select = list(options_to_be_selected.keys())
#     selected_options = 0
#     retries = 3

#     while selected_options < len(options_to_select):
#         try:
#             div_element = wait.until(
#                 EC.element_to_be_clickable((By.ID, main_div_id)))
#             div_element.click()
#             dropdown_options = wait.until(EC.visibility_of_all_elements_located(
#                 (By.CSS_SELECTOR, f"#{main_div_id} .chosen-results li")))

#             for option in dropdown_options:
#                 if option.text in options_to_be_selected and options_to_be_selected[option.text]:
#                     options_to_be_selected[option.text] = False
#                     option.click()
#                     selected_options += 1
#                     if selected_options == len(options_to_select):
#                         return
#                     time.sleep(1)
#                     break
#         except (StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException, TimeoutException):
#             retries -= 1
#             if retries == 0:
#                 raise
#             time.sleep(2)

def select_multiple_dropdown_options(main_div_id, options_to_be_selected):
    """Select multiple options from dropdown."""
    options_to_select = list(options_to_be_selected.keys())
    selected_options = 0
    retries = 3

    while selected_options < len(options_to_select):
        try:
            div_element = wait.until(
                EC.element_to_be_clickable((By.ID, main_div_id)))
            div_element.click()
            dropdown_options = wait.until(EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, f"#{main_div_id} .chosen-results li")))

            option_found = False
            for option in dropdown_options:
                if option.text in options_to_be_selected and options_to_be_selected[option.text]:
                    options_to_be_selected[option.text] = False
                    option.click()
                    selected_options += 1
                    option_found = True
                    time.sleep(1)
                    break

            if not option_found:
                # Option not found, remove it from the list
                for option_text in options_to_select:
                    if options_to_be_selected[option_text]:
                        print(f"Option '{option_text}' not found, skipping.")
                        options_to_be_selected[option_text] = False
                        selected_options += 1
                        break

            if selected_options == len(options_to_select):
                return

        except (StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException, TimeoutException):
            retries -= 1
            if retries == 0:
                raise
            time.sleep(2)
        except NoSuchElementException:
            print(f"Dropdown or options not found for ID '{
                  main_div_id}', retrying...")
            retries -= 1
            if retries == 0:
                raise
            time.sleep(2)


def click_button_with_retry(button_locator, retries=3):
    """Click a button with retry logic."""
    while retries > 0:
        try:
            button = wait.until(EC.element_to_be_clickable(button_locator))
            button.click()
            return
        except (StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException, TimeoutException):
            retries -= 1
            if retries == 0:
                raise
            time.sleep(2)


def convert_csv_to_json(download_path):
    """Convert CSV file to JSON."""
    csv_files = glob.glob(os.path.join(download_path, "Shipping*.csv"))
    if not csv_files:
        print("No CSV files found in the specified directory.")
        return

    latest_csv_file = max(csv_files, key=os.path.getctime)
    print(latest_csv_file)

    df = pd.read_csv(latest_csv_file)
    json_data = df.to_dict(orient='records')

    return json_data


def push_data_to_mongodb(data, db_name, collection_name, connection_string):
    """Push data to MongoDB."""
    try:
        client = pymongo.MongoClient(
            connection_string, tlsCAFile=certifi.where())
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

# Main script


options = webdriver.ChromeOptions()
prefs = {"download.default_directory": "/home/runner/downloads"}  # Adjust path
options.add_experimental_option("prefs", prefs)

# Run Chrome in headless mode if on Linux
if platform.system() == 'Linux':
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

try:
    driver.get("https://mymarketnews.ams.usda.gov/public_data")

    market_tab = wait.until(EC.element_to_be_clickable((By.ID, "market-tab")))
    market_tab.click()

    select_dropdown_option(
        "market_types_by_market_types_chosen", "Shipping Point")
    select_dropdown_option("drill_types_chosen", "Location - State")
    select_dropdown_option(
        "aggregate_by_market_types_section_2_chosen", "Weekly")

    input_date_element = wait.until(EC.presence_of_element_located(
        (By.ID, "from-date-by-market-types-section-2")))
    today = date.today()
    print("today ", today)
    today_str = today.strftime("%m/%d/%Y")
    input_date_element.send_keys(today_str)
    input_date_element.send_keys(Keys.RETURN)

    input_date_element = wait.until(EC.presence_of_element_located(
        (By.ID, "to-date-by-market-types-section-2")))
    seven_days_ago = today - timedelta(days=6)
    print("seven_days_ago ", seven_days_ago)
    seven_days_ago_str = seven_days_ago.strftime("%m/%d/%Y")
    input_date_element.send_keys(seven_days_ago_str)
    input_date_element.send_keys(Keys.RETURN)

    select_multiple_dropdown_options(
        "commodities_by_market_types_section_2_chosen", {"Tomatoes": True})
    select_multiple_dropdown_options("locations_by_market_types_section_2_chosen", {
                                     "Atlanta (Forest Park), Georgia": True, "Columbia, South Carolina": True, "Miami, Florida": True, "Orlando (Oviedo), Florida": True, "Raleigh, North Carolina": True})

    click_button_with_retry((By.ID, "btnGetDataMarketType"))
    click_button_with_retry((By.CSS_SELECTOR, '.btn-save-as-csv'))

    time.sleep(5)

    json_data = convert_csv_to_json(DOWNLOAD_PATH)
    push_data_to_mongodb(json_data, "Agri_Insights",
                         "tomatoPrices", MONGODB_CONNECTION_STRING)

finally:
    driver.quit()
