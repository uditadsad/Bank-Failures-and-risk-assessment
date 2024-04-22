# import mysql.connector
# import sqlalchemy
# from sqlalchemy import create_engine
# Install azure-storage-blob library
# !pip install azure-storage-blob
# pip install beautifulsoup4
# pip install azure-storage-blob
# pip install beautifulsoup4

import subprocess
subprocess.run(["pip", "install", "azure-storage-blob"])

subprocess.run(["pip", "install", "beautifulsoup4"])
import requests
from bs4 import BeautifulSoup
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def get_table_data(soup):
    table = soup.find('table')
    headers = [header.text.strip() for header in table.find_all('th')]  # Strip whitespace from headers
    rows = table.find_all('tr')
    data_rows = rows[1:]
    row_data = [[td.text.strip() for td in row.find_all('td')] for row in data_rows]  # Strip whitespace from data
    
    return pd.DataFrame(row_data, columns=headers)

def scrape_data(url):
    dataframes = []
    while url:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        dataframes.append(get_table_data(soup))
        next_link = soup.find('a', text='Next ›')
        url = f"https://www.fdic.gov{next_link['href']}" if next_link else None
    return pd.concat(dataframes)

url = "https://www.fdic.gov/resources/resolutions/bank-failures/failed-bank-list/"
data = scrape_data(url)

print("Data extracted")

# Rename DataFrame columns to match MySQL table column names
data.rename(columns={
    'Bank Name': 'BankName',
    'City': 'City',
    'State': 'State',
    'Cert': 'Cert',
    'Acquiring Institution': 'AcquiringInstitution',
    'Closing Date': 'ClosingDate',
    'Fund': 'Fund'
}, inplace=True)

print("Columns renamed")

# Define Azure Blob Storage connection parameters
azure_storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=team6store;AccountKey=h1D269iL4VKfukfklX9gKnXkD6Nv6gPAFf+lMIMITc5ThKMoVXzQtemDR4fN02LFmAYJew+Wdzjt+AStZzL32A==;EndpointSuffix=core.windows.net"
container_name = "team6container"

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Convert DataFrame to CSV string
csv_data = data.to_csv(index=False)

# Upload CSV data to Azure Blob Storage
blob_client = container_client.get_blob_client("failed_banks_data.csv")
blob_client.upload_blob(csv_data, overwrite=True)

print("Data uploaded to Azure Blob Storage successfully.")
