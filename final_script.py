# Code for ETL pipeline

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def load_to_csv(df, csv_path):
    try:
        df.to_csv(csv_path, index=False)
        print(f"CSV file saved successfully at: {csv_path}")
    except Exception as e:
        print(f"Error: {e}")


def fetch_contact_info(lastpage_url):
    # Send an HTTP request to the specified lastpage_url
    response = requests.get(lastpage_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the first tbody element on the page
        tbody = soup.find('tbody')

        # Initialize an array to store the extracted data
        contact_info = []

        # Loop through each <tr> tag inside the tbody
        for tr in tbody.find_all('tr'):
            # Find the <td> tag with class 'table_center'
            td_with_class = tr.find('td', class_='table_center')

            # Check if the <td> tag with class 'table_center' is found
            if td_with_class:
                # Extract the text content of the <td> tag
                td = td_with_class.text.strip()
                # Append the data to the array
                contact_info.append(td)
            else:
                # Handle the case when the <td> tag is not found
                print("Warning: <td> with class 'table_center' not found in a row.")

        return contact_info
    else:
        # Print an error message if the request was not successful
        print(f"Error: Unable to fetch data from {lastpage_url}")
        return None


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

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
    
    
def extract(url, table_attribs, existing_df):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df_list = []

    tables = data.find_all('tbody')

    count = 0

    while count < len(tables):
        rows = tables[count].find_all('tr')
        for row in rows:
            col = row.find_all('td')
            if len(col) >= 4:
                prefecture = col[0].get_text(strip=True)
                city = col[1].get_text(strip=True)
                school_name = col[2].get_text(strip=True)
                entrance_month = col[3].get_text(strip=True)
                lastpage_url = col[2].find('a')['href']
                print(lastpage_url)
                third_result = fetch_contact_info(lastpage_url)
                if third_result is not None and len(third_result) == 4:
                    address, tel, urel, email = third_result
                    data_dict = {"prefecture": prefecture, "CITY": city, "SCHOOL NAME": school_name,
                                "ENTRANCE MONTH": entrance_month, "ADDRESS": address, "TEL": tel,
                                "URL": urel, "EMAIL": email}
                    df_list.append(pd.DataFrame([data_dict]))

        count += 1

    if df_list:
        # Concatenate all DataFrames in the list at once
        result_df = pd.concat(df_list, ignore_index=True)
        # Concatenate the existing DataFrame with the result DataFrame
        result_df = pd.concat([existing_df, result_df], ignore_index=True)
    else:
        result_df = existing_df

    return result_df



table_attribs = ["prefecture" , "CITY" , "SCHOOL NAME" , "ENTRANCE MONTH" ,   "ADDRESS", "TEL" , "URL" , "EMAIL"]
df = pd.DataFrame(columns=table_attribs)

main_website_url = "http://code81.tv/j_language_school/"
# area_result = extract_links_from_ul(main_website_url)
# print(area_result)
area_result = ['http://code81.tv/j_language_school/kanto-area/']


for area_link in area_result:
    df = extract(area_link, table_attribs, df)
    print(df)

df_filled = df.fillna("N/A")

csv_path = './latest_language_school2.csv'
load_to_csv(df, csv_path)
