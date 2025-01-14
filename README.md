# FIGI Data API - OpenFIGI Database Builder

This project uses **OpenFIGI API** to pull financial data from the [OpenFIGI](https://www.openfigi.com/) website based on different market sectors. The data is stored as **Feather** files for efficient data storage and retrieval.

## Features
- pulls multiple pages of FIGI data for various market sectors.
- Saves data in Feather format using **Polars** for fast, memory-efficient processing.

## Dependencies

Make sure you have the following dependencies installed:

- **Python 3.x**
- [Polars](https://pypi.org/project/polars/)
- [Pandas](https://pypi.org/project/pandas/)
- 
### Install Dependencies

```bash
pip install polars pandas
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

- Modify the output path in `df.write_ipc()` to match your directory structure.
- Adjust the `time.sleep()` duration if you encounter rate-limiting issues.

