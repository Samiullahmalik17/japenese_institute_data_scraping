# Code for ETL pipeline

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
from requests.adapters import HTTPAdapter  # Correct import statement
from urllib3.util.retry import Retry


def extract_links_from_ul(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the <ul> with id="japan_map"
        japan_ul = soup.find('ul', {'id': 'japan_map'})

        # Extract all links within the <ul>
        links = [a['href'] for a in japan_ul.find_all('a')]

        return links

    except Exception as e:
        print(f"Error: {e}")
        return None

website_url = "http://code81.tv/j_language_school/"
area_result = extract_links_from_ul(website_url)

if area_result:
    print("Links:")
    for link in area_result:
        print(link)
else:
    print("Failed to retrieve links.")

# url = 'http://code81.tv/j_language_school/hokkaido-tohoku-area/'
table_attribs = ["PREFECURE" , "CITY" , "SCHOOL NAME" , "ENTRANCE MONTH" ,   "ADDRESS", "TEL" , "URL" , "EMAIL"]


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    print(len(tables))

    count = 0

    while count < len(tables):    
        rows = tables[count].find_all('tr')
        print("rowsprint",rows)
        for row in rows:
            col = row.find_all('td')
            if len(col) != 0:
                prefecure = col[0]
                city = col [1]
                school_name = col[2]
                entrance_month = col[3]
                name_col = col[2]
                name_link = name_col.find('a')
                if name_link is not None:
                    name = name_link.get('href', '')
                    mc_link = col[0].get_text(strip=True)
                    name_of_school = col[1].get_text(strip=True)
                    data_dict = {"Area ": mc_link , "PREFECURE": prefecure , "CITY": city , "SCHOOL NAME" :school_name , "ENTRANCE MONTH": entrance_month ,   "ADDRESS": address, "TEL" : tel , "URL" : urel , "EMAIL": email}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
        count +=1
    print(df)
    return df


csv_path = './latest_language_school.csv'

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

for area_link in area_result:
    df = extract(area_link, table_attribs)


load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

