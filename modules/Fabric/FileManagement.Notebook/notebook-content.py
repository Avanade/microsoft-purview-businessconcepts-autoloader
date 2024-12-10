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

import pandas as pd
from pyspark.sql import SparkSession

# Configure Azure Storage account parameters
storage_account_name = "strpurviewsaasbusconcept"
storage_account_key = ""
container_name = "strpurviewsaasbusconceptblob"
mount_point = "/mnt/strpurviewsaasbusconceptblob"

def extract_csv_as_df_from_adls(csv_name):
    
    csv_file_path = f"{mount_point}/{csv_name}"
 
    # Configure access to Azure Blob Storage
    spark.conf.set(
        f"spark.hadoop.fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
        storage_account_key
    )
    # Attempt to unmount the Blob Storage container if it is already mounted
    try:
        dbutils.fs.unmount(mount_point)
    except Exception as e:
        print(f"Error unmounting: {e}")

    # Mount the Blob Storage container to DBFS (Databricks File System)
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
        }
    )
   
    
    # Read the CSV file into a Spark DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    # Convert the Spark DataFrame to a Pandas DataFrame
    csv_df = df.toPandas()
    return csv_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
