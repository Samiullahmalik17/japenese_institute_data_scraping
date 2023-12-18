import requests
from bs4 import BeautifulSoup

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
            # Extract the text content of the <td> tag
            td = tr.find('td', class_='table_center').text.strip()

            # Append the data to the array
            contact_info.append(td)

        return contact_info
    else:
        # Print an error message if the request was not successful
        print(f"Error: Unable to fetch data from {lastpage_url}")
        return None

# Example usage:
lastpage_url = 'http://code81.tv/yoshidagakuen-computer-business-college-department-of-japanese-langage/'  # Replace this with the actual lastpage_url
third_result = fetch_contact_info(lastpage_url)


print(third_result)

