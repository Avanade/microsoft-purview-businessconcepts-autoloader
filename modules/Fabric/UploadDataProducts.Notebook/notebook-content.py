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

from requests import Session
import requests
import pandas as pd
import json

session = Session()
access_token = get_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

csv_name = "data_product.csv"
df = read_csv_from_adls_lakehouse(csv_name)
purview_guid = ""
url = f"https://{purview_guid}-api.purview-service.microsoft.com/datagovernance/catalog/dataproducts"

succeded_uploads = []
failed_uploads = []

for index, row in df.iterrows():
    try:
        # Extract and validate data
        name = row["dp_name"]
        nameDomain = row["business_domain_name"]
        status = row["dp_status"]
        type_ = row["dp_type"]
        description = row["dp_description"]
        businessUse = row["dp_businessUse"]
        contacts = row["dp_owners"]
        updateFrequency = row["dp_updateFrequency"]

        uuid = get_uuid(nameDomain)
        if not uuid:
            print(f"Invalid domain for {nameDomain}, skipping...")
            failed_uploads.append({"name": name, "error": "Invalid domain UUID"})
            continue
        
        contact_list = get_user_id(contacts)
        contact_id_list = []
        for contact in contact_list:
            contact_id_list.append({"id": contact})

        print(contact_id_list)    
            

        data_products = fetch_data_products()
        if name in data_products["name"].values.tolist():
            print(f"Data Product {name} already exists, skipping...")
            failed_uploads.append({"name": name, "error": "Data Product already exists"})
            continue

        contacts = {
            "owner": contact_id_list
        }

        payload = json.dumps({
            "name": name,
            "domain": uuid,
            "status": status,
            "type": type_,
            "description": description,
            "businessUse": businessUse,
            "contacts": contacts,
            "updateFrequency": updateFrequency
        })

        headers = {
            "dataType": "json",
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + access_token
        }

        response = session.post(url, headers=headers, data=payload)
        datasourceResponse = response.json()

        # Check if the response indicates success
        if response.status_code == 200 or response.status_code == 201 or all(key in datasourceResponse for key in ["status", "type", "id"]):
            print(f"Successfully uploaded {name}.")
            # add to the succeded uploads the name and ID of the new Data Product
            succeded_uploads.append({"name": name, "id": datasourceResponse["id"]})
        else:
            print(f"Failed to upload {name}, error: {datasourceResponse}")
            failed_uploads.append({"name": name, "error": datasourceResponse})

    except Exception as e:
        print(f"An error occurred: {e}")
        failed_uploads.append({"name": name, "error": str(e)})

# Summary of results
print(f"Succeded uploads: {len(succeded_uploads)}")
succeded_uploads_df = pd.DataFrame(succeded_uploads)
#print(succeded_uploads_df)
#print(f"Failed uploads: {len(failed_uploads)}")
#failed_uploads_df = pd.DataFrame(failed_uploads)
#display(failed_uploads_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
