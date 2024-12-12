from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

import polars as pl
import gc

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

webdriver_service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)

def tables(type_code):
    driver.get(f"https://www.openfigi.com/search#!?page=1&pageSize=100&marketSector={type_code}&facets=")
    time.sleep(2)

    # Check Page Numbers
    span_element = driver.find_element(By.CSS_SELECTOR, "div.row._form-inline")
    span_element = span_element.find_element(By.CSS_SELECTOR, "span[ng-bind='state.totalPages | thousands']")
    total_pages = span_element.text
    total_pages = int(total_pages.replace(',', ''))

    page_number = 1
    table_data = []
    max_columns = 6  # Number of headers

    while page_number <= total_pages:
        driver.get(f"https://www.openfigi.com/search#!?page={page_number}&pageSize=100&marketSector={type_code}&facets=")
        time.sleep(0.52)

        table = driver.find_element(By.CLASS_NAME, "table.table-bordered")
        rows = table.find_elements(By.TAG_NAME, "tr")

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            row_data = [col.text for col in cols]
            while len(row_data) < max_columns:
                row_data.append(None)  # Pad missing columns with None
            table_data.append(row_data)

        page_number += 1
        print(f"{type_code} - Page {page_number} of {total_pages} Complete", end='\r')

    headers = ['FIGI', 'NAME', 'TICKER', 'EXCHANGE_ID', 'SECURITY_TYPE', 'MARKET_TYPE']

    df = pl.DataFrame(table_data, schema=headers, orient="row")
    df = df.drop_nulls()
    df.write_ipc(f'ENTER_FILE_PATH_HERE/DB_{type_code}.feather')

    del table_data
    del df
    gc.collect()


def run():

    links = ['Comdty','Corp', 'Curncy', 'Equity','Govt', 'Index', 'M-Mkt', 'Mtge', 'Muni', 'Pfd']

    for link in links:
        tables(link)

run()

driver.quit()
