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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def remove_html_tags(text):
    """Remove html tags from a string"""
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_html_tags(text):
    """Remove html tags from a string"""
    div_txt = f"<div>{text}</div>"
    return div_txt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_products = fetch_data_products()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#read from CSV
csv_name = "data_assets.csv"
df = read_csv_from_adls_lakehouse(csv_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined = df.merge(data_products, how="inner",left_on="dp_name",right_on="name")
display(df_joined)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for index, row in df_joined.iterrows():
    tenant_id = "467750fa-44ec-49d4-ba43-5ae49d501676"
    fqn = row["data_asset_fqn"]
    guid = get_id(fqn,row["data_asset_purview_account"])
    purview_account = row["data_asset_purview_account"]
    newGuid, description = copy_asset(fqn,guid,tenant_id,purview_account)
    if newGuid == None or description == None:
        continue
    else:
    #Relate Data Asset to Data Product
        relatedData = {
        "entityId": newGuid,
        "description": description,
        "relationshipType": "Related",
        }

        url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/dataproducts/{row['id']}/relationships?entityType=DataAsset"
        headers = { "dataType": "json", "accept": "application/json", "Content-Type": "application/json", "Authorization": "Bearer " + access_token }
        payload = json.dumps(relatedData)

        relationResponse = requests.request("Post", url, headers=headers, data=payload)
        relationJson = relationResponse.json()

        if relationResponse.status_code == 200 or  relationResponse.status_code == 201:
            print(relationResponse.status_code,":Asset",fqn," added to DataProduct ",row["name"]," successfully")
        else:
            print("Error adding Asset to Data Product: ",relationJson)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
