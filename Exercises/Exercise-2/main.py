from typing import Optional
import requests
import pandas as pd
import logging
from bs4 import BeautifulSoup
from io import StringIO
import argparse

# Create and configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler('dumb.log')
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.WARNING)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
file_target = "2022-02-07 14:03" # Doesn't exist as it was modified since then! -> I'll use the following time
file_target_updated = "2024-01-19 10:39"


def scrape(url:str, target_date: str) -> Optional[str]:
    r = requests.request('GET', url=url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find("table")
    for row in table.find_all("tr"):
        logger.info(row)
        tds = row.find_all("td")
        logger.info(tds)
        if len(tds) < 2:
            logger.debug("Skipping a row as it doesn't have enough columns to be relevant.")
            continue
        if tds[1].text == f"{target_date}  ": # The space at the end is important to match the exact timestamp in the table!
            logger.info(f"Match found: {tds[0].text.strip()}")
            return url+tds[0].text.strip()
        else:
            logger.info(f"Skipping row: '{tds[0].text.strip()}', as it does not match the target timestamp.")
    logger.info("finished iterating over all the csv files! no match was found")


def pandize(final_url:str) -> Optional[pd.DataFrame]:
    try:
        response = requests.request('GET', url=final_url)
        response.raise_for_status()  # Raises an HTTPError if the response was unsuccessful
    except requests.exceptions.RequestException as e:
        logger.exception(f"An error occurred: {e}")
        return None
    if response.status_code ==200:
        csv_data=StringIO(response.text)
        return pd.read_csv(csv_data,low_memory=False)
    else:
        logger.info("The request to get the CSV file is not successful")
        return

def print_max_specific_column_record(df):
    max_record = df.iloc[df['HourlyDryBulbTemperature'].idxmax()]
    print(max_record[['STATION', 'DATE', 'HourlyDryBulbTemperature']])

def main():
    parser = argparse.ArgumentParser(description='Scrape and download NOAA weather data.')
    parser.add_argument('--date', type=str, required=True, help='Last modified date to search for.')
    args = parser.parse_args()

    final_url = scrape(url, args.date)
    if final_url is None:
        logger.error("No file found with the specified timestamp.")
        return
    df = pandize(final_url)
    if df is not None:
        print_max_specific_column_record(df)
    else:
        logger.error("Failed to download and process the CSV file.")

if __name__ == "__main__":
    main()
