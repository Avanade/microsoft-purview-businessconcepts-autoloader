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

%run GetUserDetails 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run FileManagement

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run BusinessDetails

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from requests import Session
session = Session()
from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_okrs(domainid):
  access_token = get_token()
  """Fetch business domains from Purview API."""
  url = f"https://{PURVIEW_GUID}-api.purview-service.microsoft.com/datagovernance/catalog/objectives?domainId={domainid}&orderby=definition+asc&skip=0&top=25"
  
  headers = {
      "dataType": "json",
      "accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": f"Bearer {access_token}"
  }
  
  response = session.get(url, headers=headers, stream=True)
  response.raise_for_status()  # Raise an exception for HTTP errors
  okrs = response.json().get("value", [])
  
  okr_records = [
    [okr.get("id"), okr.get("definition"), okr.get("status"), okr.get("domain")]
    for okr in okrs
  ]
  return pd.DataFrame(okr_records, columns=['id', 'definition', 'status', 'domain'])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

okr_csv_df = read_csv_from_adls_lakehouse("okr.csv")

access_token = get_token()

purview_guid = ""
url = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/objectives"

succeded_uploads = []
failed_uploads = []

for index, row in okr_csv_df.iterrows():
    #try:
        # Extract and validate data
        definition = row["definition"]
        domain = row["domain"]
       
        targetDate = datetime.strptime(row["targetDate"],"%m-%d-%Y")
        targetDate = targetDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # Convert date to string
        owners = row["owner"]
        status=row["status"]
        
        uuid = get_uuid(domain)
        if not uuid:
            print(f"Invalid domain for {domain}, skipping...")
            failed_uploads.append({"definition": definition, "error": "Invalid domain UUID"})
            continue
        
        owners_list = owners.split (",")  
        owner_json = ""      
        for owner in owners_list:
            owner_id = get_user_id(owner)
            if not owner_id:
                print(f"User {owner} not found, skipping...")
                failed_uploads.append({"definition": definition, "error": "Invalid user ID"})
                continue
            owner_json = owner_json + ',' +  f'{{"id": "{owner_id}"}}'

        owner_json = owner_json.lstrip(',')
        
        contacts =f'{{"owner": [{owner_json}]}}'
        contacts = json.loads(contacts)
        #print(contacts)
        okrs = fetch_okrs(uuid)

        if definition in okrs["definition"].values.tolist():
            print(f"OKR {definition} already exists, skipping...")
            failed_uploads.append({"definition": definition, "error": "OKR already exists"})
            continue
        
        payload = json.dumps({
            "definition": definition,
            "domain": uuid,
            "targetDate": targetDate,
            "contacts" : contacts,
             "status": status
               })

        headers = {
            "dataType": "json",
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + access_token
        }
        #print(payload)
        response = session.post(url, headers=headers, data=payload)
        datasourceResponse = response.json()

        # Check if the response indicates success
        if response.status_code == 200 or response.status_code == 201:
            print(f"Successfully uploaded {definition}.")
            succeded_uploads.append({"definition": definition, "id": datasourceResponse["id"]})
        else:
            print(f"Failed to upload {definition}, error: {response.status_code} {datasourceResponse}")
            failed_uploads.append({"name": definition, "error": datasourceResponse})
    #except Exception as e:
    #    print(f"An error occurred: {e}")
    #    failed_uploads.append({"definition": definition, "error": str(e)})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
