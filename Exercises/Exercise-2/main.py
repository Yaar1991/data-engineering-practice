from typing import Optional
import requests
import pandas as pd
import logging
from bs4 import BeautifulSoup
from io import StringIO

# Create and configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler('dumb.log')
stream_handler = logging.StreamHandler()

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


def scrape(url:str):
    r = requests.request('GET', url=url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find("table")
    for row in table.find_all("tr"):
        tds = row.find_all("td")
        logger.info(tds)
        if len(tds) > 1:
            if tds[1].text == f"{file_target_updated}  ":
                logger.info("found the relevant csv file by the modified by value!\nStripping the url")
                first_td_val = tds[0].text.strip()
                logger.info(f"Match found: {first_td_val}")
                final_url = url+first_td_val
                return final_url
            else:
                continue
        else:
            logger.info(f"not suitable for the use case!")
    logger.info("finished iterating over all the csv files! no match was find")


def pandize(final_url:str) -> Optional[pd.DataFrame]:
    try:
        response = requests.request('GET', url=final_url)
    except Exception as e:
        logger.exception(e)
    if response.status_code ==200:
        # with open()
        csv_data=StringIO(response.text)
        return pd.read_csv(csv_data)
    else:
        logger.info("The request to get the CSV file is not successful")
        return

def print_max_specific_column_record(df):
    logger.info(df.iloc[df['HourlyDryBulbTemperature'].idxmax()])

def main():
    final_url = scrape(url)
    df = pandize(final_url)
    print_max_specific_column_record(df)


if __name__ == "__main__":
    main()
