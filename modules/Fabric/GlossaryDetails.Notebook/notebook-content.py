# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "df568330-e635-42ae-9be5-7f0b70e22777",
# META       "default_lakehouse_name": "Purview_Lakehouse",
# META       "default_lakehouse_workspace_id": "cf8bb0d9-a61b-4a31-9f6a-37474df4f23a"
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
import pandas as pd
from requests import Session

# Configuration constants
SERVICE_PRINCIPAL_ID = "ec7313b1-dc75-44d6-a248-1412d06c9f55"
SERVICE_PRINCIPAL_SECRET_NAME = "45L8Q~B_8t36C~LYhtUDoz6BPDG8D8Uc-4qCqaBc"
TENANT_ID = "467750fa-44ec-49d4-ba43-5ae49d501676"
GRANT_TYPE = "client_credentials"
RESOURCE = "https://purview.azure.net"
PURVIEW_GUID = "467750fa-44ec-49d4-ba43-5ae49d501676"

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



def create_dataframe_from_glossary(glossary):
  """Create a DataFrame from the list of domains."""
  records = [
      [term.get("id"), term.get("name"), term.get("domain"), term.get("parentId")]
      for term in glossary
  ]
  return pd.DataFrame(records, columns=['id', 'name', 'domain', 'parentId'])

def get_glossary_id(glossary_name):
  """Retrieve the UUID of a domain by name."""
  for record in fetch_glossary().values.tolist():
      if record[1] == glossary_name:
          return record[0]
  print(f"Glossary {glossary_name} not found in list")
  return None

def get_glossary_id(glossary_name,domain_uuid):
  """Retrieve the UUID of a domain by name."""
  if domain_uuid:
    print("DomaninID---"+domain_uuid)
    for record in fetch_glossary(domain_uuid).values.tolist():
      if record[1] == glossary_name:
          return record[0]
    print(f"Glossary {glossary_name} not found in list")
  return None

def get_glossary_domain(glossary_name):
  """Retrieve the UUID of a domain by name."""
  for record in fetch_glossary().values.tolist():
      if record[1] == glossary_name:
          return record[2]
  print(f"Glossary {glossary_name} not found in list")
  return None  

def get_glossary_parentid(glossary_name):
  """Retrieve the UUID of a domain by name."""
  for record in fetch_glossary().values.tolist():
      if record[1] == glossary_name:
          return record[3]
  print(f"Glossary {glossary_name} not found in list")
  return None  

def fetch_glossary(domainIdValues = []):
  """
  Fetch the list of domains from Purview glossary API.
  """
  access_token = get_token()
  headers = {
        "dataType": "json",
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

  if domainIdValues:
    url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/query"
    domainIdValues = [domainIdValues]
    filterdomainIds = {"domainIds": domainIdValues}
    payload = json.dumps(filterdomainIds)
    response = session.post(url, headers=headers, stream=True,data=payload)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return create_dataframe_from_glossary(response.json().get("value", []))
  else:    
    """Fetch  from Purview glossary API."""
    url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/"
    response = session.get(url, headers=headers, stream=True)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return create_dataframe_from_glossary(response.json().get("value", []))


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
