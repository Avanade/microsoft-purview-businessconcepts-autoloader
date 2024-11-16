# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

access_token = get_token()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

service_principal_id = "ec7313b1-dc75-44d6-a248-1412d06c9f55"
service_principal_secret_name = "45L8Q~B_8t36C~LYhtUDoz6BPDG8D8Uc-4qCqaBc"
tenant_id = "467750fa-44ec-49d4-ba43-5ae49d501676"
# Replace with the user ID you want to retrieve
user_email = 'manish.a.gonsalves@avanade.com'
grant_type = "client_credentials"
resource = "https://purview.azure.net"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from pyspark.sql.functions import col, explode, when

def get_id(fqn: str,purview_account: str) -> str:
  '''Performs a query to the data map API to retrieve the ID of a given FQN.'''
  url = f"https://{purview_account}.purview.azure.com//datamap/api/search/query?api-version=2023-09-01"

  payload = json.dumps({
  "keywords": None,
  "limit": 10,
  "filter": {
    "and": [
      {
        "not": {
          "objectType": "Glossary terms"
        }
      },
      {
        "attributeName": "qualifiedName",
        "operator": "eq",
        "attributeValue": fqn
      }
    ]
  }
})
  headers = { "dataType": "json", "accept": "application/json", "Content-Type": "application/json", "Authorization": "Bearer " + access_token }


  response = requests.request("POST", url, headers=headers, data=payload)
  if response.status_code != 200:
  #print(response.text)
    errTxt = (response.status_code,":",response.text)
    return errTxt
  else:
    # Convert JSON to DataFrame
    df = spark.createDataFrame(response.json()['value'])
    # Filter rows where 'qualifiedName' matches variable x and select the 'id'
    df_filtered = df.filter(col("qualifiedName") == fqn).select("id")
    return df_filtered.collect()[0]["id"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
