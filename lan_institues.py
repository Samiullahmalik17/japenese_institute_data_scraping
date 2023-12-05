# Code for ETL pipeline

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
from requests.adapters import HTTPAdapter  # Correct import statement
from urllib3.util.retry import Retry

url = 'https://hh-japaneeds.com/schools/#Kyushu_region'
table_attribs = ["Area ","Language school", "Language school link"]


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    print(len(tables))

    count = 0

    while count < 44:    
        rows = tables[count].find_all('tr')
        for row in rows:
            col = row.find_all('td')
            if len(col) != 0:
                name_col = col[1]
                name_link = name_col.find('a')
                if name_link is not None:
                    name = name_link.get('href', '')
                    mc_link = col[0].get_text(strip=True)
                    name_of_school = col[1].get_text(strip=True)
                    data_dict = {"Area ": mc_link , "Language school":name_of_school,"Language school link": name}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
        count +=1
    print(df)
    return df


csv_path = './lang_school.csv'

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')


# # Function to extract details with retries
# def extract_details_with_retry(row):
#     url = row['Language school link']

#     # Setting up retries
#     session = requests.Session()
#     retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
#     session.mount('http://', HTTPAdapter(max_retries=retries))

#     try:
#         response = session.get(url)
#         response.raise_for_status()
#         page = response.text
#         soup = BeautifulSoup(page, 'html.parser')

#         # Extracting address and email based on the HTML structure of the website
#         address = soup.find('div', class_='address-class')  # Adjust the class based on the actual HTML structure
#         email = soup.find('a', href='mailto:')  # Adjust based on how emails are displayed on the website

#         address_text = address.get_text(strip=True) if address else 'N/A'
#         email_text = email.get('href').replace('mailto:', '') if email else 'N/A'

#         return pd.Series({'Address': address_text, 'Email': email_text})

#     except requests.exceptions.RequestException as e:
#         print(f"Error accessing {url}: {e}")
#         return pd.Series({'Address': 'N/A', 'Email': 'N/A'})

# # Apply the extract_details_with_retry function to each row in the DataFrame
# details_df = df.apply(extract_details_with_retry, axis=1)

# # Concatenate the original DataFrame with the new details DataFrame
# df = pd.concat([df, details_df], axis=1)

# # Save the updated DataFrame to CSV
# load_to_csv(df, csv_path)