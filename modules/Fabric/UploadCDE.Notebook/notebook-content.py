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
# META     },
# META     "environment": {}
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

%run BusinessDetails

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

%run getIDfromFQN

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run copyAsset

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from requests import Session
session = Session()
from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
purview_guid = ""
purview_account_name ="prvw-clothesrus"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get all existing Critical Data Elements for a Domain GUID
def fetch_cdes(domainid):
  access_token = get_token()
  """Fetch business domains from Purview API."""
  url = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/criticalDataElements?domainId={domainid}"
  
  headers = {
      "dataType": "json",
      "accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": f"Bearer {access_token}"
  }
  
  response = session.get(url, headers=headers, stream=True)
  response.raise_for_status()  # Raise an exception for HTTP errors
  cdes = response.json().get("value", [])
  
  cde_records = [
    [cde.get("name"), cde.get("domain"), cde.get("dataType"), cde.get("status"),cde.get("id")]
    for cde in cdes
  ]
  return pd.DataFrame(cde_records, columns=['name', 'domain', 'dataType','status','id'])

def fetch_dataasset_details(dataassetID):
 
  access_token = get_token()
  
  
  url = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/dataassets/{dataassetID}"
  
  headers = {
      "Accept": "application/json",
      "Content-Type": "application/json",
      "Authorization": f"Bearer {access_token}"
  }
  
  payload= json.dumps({
            
            })
   

  #print(payload)
  response = requests.get(url, headers=headers, data=payload)
  response.raise_for_status()  # Raise an exception for HTTP errors
  dataasset = response.json()
  
  return dataasset
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cde_csv_df = read_csv_from_adls_lakehouse("cde.csv")

access_token = get_token()


url = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/criticalDataElements"
headers = {
            "dataType": "json",
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + access_token
        }

succeded_uploads = []
failed_uploads = []

for index, row in cde_csv_df.iterrows():
    #try:
        # Extract and validate data
        name = row["cde_name"]
        domain = row["business_domain_name"]
        datatype = row["cde_dataType"]
        owners = row["cde_owner"]
        description = row["cde_description"]
        status=row["cde_status"]
        fqdn=row["cde_fqdn"]
        columnname = row["cde_column_name"]

        #Get the DataMapAssetID through an ATLAS API search query for the given FQDN
        dataMapAssetIDs = get_id(fqdn,purview_account_name) 
       
        print("Data MAP asset ID is "+dataMapAssetIDs)
        if not dataMapAssetIDs:
            print(f"Asset not found for {fqdn} in the Data Map, adding column to CDE will be skipped...")
        else:
            datacatgalogassetid = copy_asset(fqdn,dataMapAssetIDs,purview_guid ,purview_account_name)
            
        if not datacatgalogassetid:
            print(f"Asset not found or could not be created for {fqdn} in the Data Catalog, adding column to CDE will be skipped...")
        else:
            dataassetid = datacatgalogassetid[0]
            print("dataassetid is "+dataassetid)
        
        
        uuid = get_uuid(domain)
        if not uuid:
            print(f"Invalid domain for {domain}, skipping...")
            failed_uploads.append({"name": name, "error": "Invalid domain UUID"})
            continue
        
        owners_list = owners.split (",")  
        owner_json = ""      
        for owner in owners_list:
            owner_id = get_user_id(owner)
            if not owner_id:
                print(f"User {owner} not found, skipping...")
                failed_uploads.append({"name": name, "error": "Invalid user ID"})
                continue
            owner_json = owner_json + ',' +  f'{{"id": "{owner_id}"}}'

        owner_json = owner_json.lstrip(',')
        
        contacts =f'{{"owner": [{owner_json}]}}'
        contacts = json.loads(contacts)
        cdes = fetch_cdes(uuid)
        
        payload= json.dumps({
            "name": name,
            "dataType": datatype,
            "status": status,
            "contacts" : contacts,
            "description": description,
            "domain": uuid  
            })
   

        if name not in cdes["name"].values.tolist():
            print(f"Adding CDE {name} ...")
            response = session.post(url, headers=headers, data=payload)
            datasourceResponse = response.json()
            cde_id=datasourceResponse.get("id", [])
            print("cde_id is "+cde_id)
        else:
            print(f"CDE {name} already exists, updating...") 
            cde_id = cdes.loc[cdes['name'] == name, 'id'].iloc[0]
            print("cde_id is "+cde_id)
    
        #if there is a valid dataassetID then add the relationships to the CDE
        if dataassetid:
            #Get the data asset column details 
            dataasset_schema_details = fetch_dataasset_details(dataassetid).get("schema")
            if dataasset_schema_details:
                dataasset_name = fetch_dataasset_details(dataassetid).get("name")
                dataasset_records = [
                    [dataasset_name, dataassetschema.get("name"), dataassetschema.get("description"), dataassetschema.get("classifications"), dataassetschema.get("type")]
                    for dataassetschema in dataasset_schema_details
                    ]
                dataasset_records_df = pd.DataFrame(dataasset_records, columns=['assetName','name', 'description', 'classifications', 'type'])
                dataasset_record_df = dataasset_records_df.loc[dataasset_records_df["name"].str.lower() == columnname.lower()]
                if not dataasset_record_df.empty:
                    #Proceed with relationhip adding only if schema details of the column are found
                    #display(dataasset_record_df)
                    criticalDataColumnsurl = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/criticalDataColumns"
                    payloadcriticalDataColumns = json.dumps ({
                        "name":dataasset_record_df["name"].iloc[0],
                        "dataType":dataasset_record_df["type"].iloc[0],
                        "description":dataasset_record_df["description"].iloc[0],
                        "classifications":dataasset_record_df["classifications"].iloc[0],
                        "assetName":dataasset_record_df["assetName"].iloc[0],
                        "assetId": dataassetid,
                        "domain": uuid})
                    #print("payloadcriticalDataColumns-->"+payloadcriticalDataColumns)
                    response = session.post(criticalDataColumnsurl, headers=headers, data=payloadcriticalDataColumns)
                    criticalDataColumnID = response.json().get("id", [])
                    #print("criticalDataColumnID is "+ str(criticalDataColumnID))

                    criticalDataElementsRelationshipurl = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/criticalDataElements/{cde_id}/relationships?entityType=CriticalDataColumn"
                    payloadCriticalDataElement = json.dumps ({
                        "entityId":criticalDataColumnID,
                        "description":"",
                        "relationshipType":"Related"}
                    )
                    response = session.post(criticalDataElementsRelationshipurl, headers=headers, data=payloadCriticalDataElement)
                    #print("criticalDataElementsRelationshipurl is" +criticalDataElementsRelationshipurl)
                    #print("payloadCriticalDataElement " +payloadCriticalDataElement)

                    dataAssetRelationshipurl =f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/dataAssets/{dataassetid}/relationships?entityType=CriticalDataColumn"
                    payloadDataAssetRelationship =json.dumps ({
                        "entityId":criticalDataColumnID,
                        "description":"",
                        "relationshipType":"Related"}
                        )
                    response = session.post(dataAssetRelationshipurl, headers=headers, data=payloadDataAssetRelationship)
                    datasourceResponse = response.json()
               

        # Check if the response indicates success
        if response.status_code == 200 or response.status_code == 201:
            print(f"Successfully uploaded {name}.")
            succeded_uploads.append({"name": name, "id": datasourceResponse})
        else:
            print(f"Failed to upload {name}, error: {response.status_code} {datasourceResponse}")
            failed_uploads.append({"name": name, "error": datasourceResponse})
    #except Exception as e:
    #    print(f"An error occurred: {e}")
    #    failed_uploads.append({"definition": name, "error": str(e)})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
