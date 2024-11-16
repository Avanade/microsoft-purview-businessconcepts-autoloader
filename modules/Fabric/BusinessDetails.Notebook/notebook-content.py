# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
import pandas as pd
from requests import Session

# Configuration constants
SERVICE_PRINCIPAL_ID = ""
SERVICE_PRINCIPAL_SECRET_NAME = ""
TENANT_ID = ""
GRANT_TYPE = "client_credentials"
RESOURCE = "https://purview.azure.net"
PURVIEW_GUID = ""

# Initialize session
session = Session()

def get_token():
  """Retrieve an OAuth2 token."""
  url_azure = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
  payload_azure = f'grant_type={GRANT_TYPE}&client_id={SERVICE_PRINCIPAL_ID}&client_secret={SERVICE_PRINCIPAL_SECRET_NAME}&resource={RESOURCE}'
  headers_azure = {'Content-Type': 'application/x-www-form-urlencoded'}
  
  response = session.post(url_azure, headers=headers_azure, data=payload_azure, stream=True)
  response.raise_for_status()  # Raise an exception for HTTP errors
  token_data = response.json()
  return token_data.get("access_token")

def fetch_business_domains():
  access_token = get_token()
  """Fetch business domains from Purview API."""
  url = f"https://{PURVIEW_GUID}-api.purview-service.microsoft.com/datagovernance/catalog/businessdomains"
  headers = {
      "dataType": "json",
      "accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": f"Bearer {access_token}"
  }
  
  response = session.get(url, headers=headers, stream=True)
  response.raise_for_status()  # Raise an exception for HTTP errors
  return create_dataframe_from_domains(response.json().get("value", []))

def fetch_data_assets():
  access_token = get_token()
  """Fetch dataassets from Purview API."""
  url = f"https://{PURVIEW_GUID}-api.purview-service.microsoft.com/datagovernance/catalog/dataassets"
  headers = {
      "dataType": "json",
      "accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": f"Bearer {access_token}"
  }
  
  response = session.get(url, headers=headers, stream=True)
  response.raise_for_status()  # Raise an exception for HTTP errors
  return create_dataframe_from_domains(response.json().get("value", []))

def fetch_data_products():
  access_token = get_token()
  """Fetch dataproducts from Purview API."""
  url = f"https://{PURVIEW_GUID}-api.purview-service.microsoft.com/datagovernance/catalog/dataproducts"
  headers = {
      "dataType": "json",
      "accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": f"Bearer {access_token}"
  }
  
  response = session.get(url, headers=headers, stream=True)
  response.raise_for_status()  # Raise an exception for HTTP errors
  return create_dataframe_from_domains(response.json().get("value", []))

def create_dataframe_from_domains(domains):
  """Create a DataFrame from the list of domains."""
  records = [
      [domain.get("id"), domain.get("name"), domain.get("type"), json.dumps(domain.get("domains"))]
      for domain in domains
  ]
  return pd.DataFrame(records, columns=['id', 'name', 'type', 'domains'])

def get_uuid(domain_name):
  """Retrieve the UUID of a domain by name."""
  for record in fetch_business_domains().values.tolist():
      if record[1] == domain_name:
          return record[0]
  print(f"Domain {domain_name} not found in list")
  return None

# Main execution for testing the functions
if __name__ == "__main__":
  # Example usage of get_uuid function
  #domain_name = 'Product'
  #uuid = get_uuid(domain_name)
  #print(f"UUID for {domain_name}: {uuid}")
  #display(fetch_business_domains())
  #display(fetch_data_products())
  pass


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
