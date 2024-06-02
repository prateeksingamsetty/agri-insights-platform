from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, ElementClickInterceptedException
import time


PATH = "C:\Program Files (x86)\chromedriver.exe"
driver = webdriver.Chrome(PATH)

driver.get("https://mymarketnews.ams.usda.gov/public_data")

# Wait until the page is loaded
wait = WebDriverWait(driver, 20)

#Function to select options from dropdowns
def select_dropdown_element(main_div_id, option_to_be_selected):
    div_element = wait.until(EC.element_to_be_clickable((By.ID, main_div_id)))
    div_element.click()
    dropdown_options = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "#"+ main_div_id +" .chosen-results li")))
    for option in dropdown_options:
        if option.text == option_to_be_selected:
            option.click()
            break

#Function to select multiple options from dropdowns
def select_multiple_dropdown_element(main_div_id, options_to_be_selected):
    div_element = wait.until(EC.element_to_be_clickable((By.ID, main_div_id)))
    div_element.click()
    
    options_to_select = list(options_to_be_selected.keys())
    selected_options = 0

    while selected_options < len(options_to_select):
        try:
            dropdown_options = wait.until(
                EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "#" + main_div_id + " .chosen-results li"))
            )

            for option in dropdown_options:
                if option.text in options_to_be_selected and options_to_be_selected[option.text]:
                    options_to_be_selected[option.text] = False
                    option.click()
                    selected_options += 1
                    if selected_options == len(options_to_select):
                        return
                    time.sleep(1)  # Slight delay to ensure dropdown reopens
                    div_element = wait.until(EC.element_to_be_clickable((By.ID, main_div_id)))
                    div_element.click()
                    break
        except StaleElementReferenceException:
            # Reopen the dropdown if a stale element exception is caught
            div_element = wait.until(EC.element_to_be_clickable((By.ID, main_div_id)))
            div_element.click()


#Open the tab you want to access in the webpage
market_tab = wait.until(EC.element_to_be_clickable((By.ID, "market-tab")))
market_tab.click()

select_dropdown_element("market_types_by_market_types_chosen", "Shipping Point")
select_dropdown_element("drill_types_chosen", "Location - State")
select_dropdown_element("aggregate_by_market_types_section_2_chosen", "Weekly")

#Now choose the start time you want in MM/DD/YYYY format
input_date_element = wait.until(EC.presence_of_element_located((By.ID, "from-date-by-market-types-section-2")))
input_date_element.send_keys("05/23/2019")
input_date_element.send_keys(Keys.RETURN)

#Now choose the end time you want in MM/DD/YYYY format
input_date_element = wait.until(EC.presence_of_element_located((By.ID, "to-date-by-market-types-section-2")))
input_date_element.send_keys("05/23/2024")
input_date_element.send_keys(Keys.RETURN)

#Choose the Commodities you want here
select_multiple_dropdown_element("commodities_by_market_types_section_2_chosen", {"Tomatoes":True})
select_multiple_dropdown_element("locations_by_market_types_section_2_chosen", {"Atlanta (Forest Park), Georgia": True,"Columbia, South Carolina": True,"Miami, Florida": True,"Orlando (Oviedo), Florida": True,"Raleigh, North Carolina": True})

#Finally click the Get Data Button
get_data_button = wait.until(EC.element_to_be_clickable((By.ID, "btnGetDataMarketType")))
get_data_button.click()

#Later, download the generated CSV file
csv_file_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.btn-save-as-csv')))
csv_file_button.click()

#Wait for few seconds for the file to download and then close the tab
time.sleep(5)

driver.quit()