# Selenium FIGI Data Scraper - OpenFIGI Database Builder

This project uses **Selenium** to scrape financial data from the [OpenFIGI](https://www.openfigi.com/) website based on different market sectors. The scraped data is stored as **Feather** files for efficient data storage and retrieval.

## Features
- Scrapes multiple pages of FIGI data for various market sectors.
- Headless Chrome browser for seamless background execution.
- Saves data in Feather format using **Polars** for fast, memory-efficient processing.

## Dependencies

Make sure you have the following dependencies installed:

- **Python 3.x**
- [Selenium](https://pypi.org/project/selenium/)
- [Polars](https://pypi.org/project/polars/)
- [Pandas](https://pypi.org/project/pandas/)
- [Webdriver Manager](https://pypi.org/project/webdriver-manager/)
- Google Chrome and [ChromeDriver](https://sites.google.com/chromium.org/driver/)

### Install Dependencies

```bash
pip install selenium polars pandas webdriver-manager
```

## How the Code Works

### 1. Configure ChromeDriver

The script sets up a headless Chrome browser using Selenium:

```python
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

webdriver_service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
```

### 2. Scrape Table Data

The `tables(type_code)` function scrapes FIGI data based on the market sector provided:

```python
def tables(type_code):
    driver.get(f"https://www.openfigi.com/search#!?page=1&pageSize=100&marketSector={type_code}&facets=")
    time.sleep(2)
    ...
```

It navigates through multiple pages, scrapes table rows, and saves the data into a Feather file:

```python
df.write_ipc(f'C:/Users/Brayden Boyko/OneDrive/BOYKO TERMINAL/PROGRAMS/Data/Data Hub/TERMINAL/DB_{type_code}.feather')
```

### 3. Run the Scraper

The `run()` function iterates over a list of market sector codes and calls `tables()` for each:

```python
def run():
    links = ['Comdty','Corp', 'Curncy', 'Equity','Govt', 'Index', 'M-Mkt', 'Mtge', 'Muni', 'Pfd']
    for link in links:
        tables(link)

run()
```

### 4. Clean Up

The driver is closed after the scraping process:

```python
driver.quit()
```

## Market Sectors

The script scrapes data for the following market sectors:

- **Comdty**: Commodities
- **Corp**: Corporate Securities
- **Curncy**: Currencies
- **Equity**: Equities
- **Govt**: Government Securities
- **Index**: Indices
- **M-Mkt**: Money Markets
- **Mtge**: Mortgages
- **Muni**: Municipal Bonds
- **Pfd**: Preferred Securities

## Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/selenium-figi-scraper.git
   cd selenium-figi-scraper
   ```

2. Run the script:

   ```bash
   python scraper.py
   ```

3. The Feather files will be saved to the specified output directory.

## Notes

- Ensure you have **Google Chrome** installed.
- Modify the output path in `df.write_ipc()` to match your directory structure.
- Adjust the `time.sleep()` duration if you encounter rate-limiting issues.

