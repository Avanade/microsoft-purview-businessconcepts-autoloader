# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

def copy_asset(fqn, asset_guid,tenant_id,accountName) -> str: 
    #Returns the new guid and description of the asset either created or retreived
    #If type of asset is not supported it will send back a None,None pair
    access_token = get_token()
    url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/dataassets/query"
    headers = {
        "dataType": "json",
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + access_token,
    }
    payload = json.dumps({"dataMapAssetIds": [asset_guid], "top": 1})
    response = requests.request("POST", url, headers=headers, data=payload)
    queryData = response.json()
    
    if queryData.get('count', 0) != 0:
        print("DataCatalogue Asset",fqn," already created")
        newGuid = queryData["value"][0]["id"] 
        description = queryData["value"][0]["description"]
        #does the thing of just relating it to the Data Product, skip everything else
    else:
        print("DataCatalogue Asset",fqn,"doesnt exist, creating...")
        headers = { "dataType": "json", "accept": "application/json", "Content-Type": "application/json", "Authorization": "Bearer " + access_token }
        payload = {}

        url = f"https://{tenant_id}-api.purview-service.microsoft.com/catalog/api/atlas/v2/entity/bulk?guid={guid}&ignoreRelationships=true&includeTermsInMinExtInfo=true"
        response = requests.request("GET", url, headers=headers, data=payload)
        simpleData = response.json()
        
        AssetType = simpleData["entities"][0]["typeName"]
        AssetName = simpleData["entities"][0]["attributes"]["displayName"]
        
        url = f"https://{tenant_id}-api.purview-service.microsoft.com/catalog/api/atlas/v2/entity/bulk?excludeRelationshipTypes=dataset_process_inputs&excludeRelationshipTypes=process_dataset_outputs&excludeRelationshipTypes=process_parent&excludeRelationshipTypes=direct_lineage_dataset_dataset&guid={guid}&includeTermsInMinExtInfo=true&minExtInfo=true"
        response = requests.request("GET", url, headers=headers, data=payload)
        Assetdata = response.json()
        
        url = f"https://{tenant_id}-api.purview-service.microsoft.com/catalog/api/atlas/v2/lineage/{guid}?direction=BOTH&forceNewApi=true&includeParent=true&width=6&getDerivedLineage=false"
        response = requests.request("GET", url, headers=headers, data=payload)
        lineageData = response.json()

        schemaList= []
        dataClassifications = []
        
        match AssetType:
            ############
            #azure_sql_table
            ############
            case "azure_sql_table":
                print("azure sql table")
                #Static Type
                newDataType = "AzureSqlTable"
                newDataFormat = "Table"
                #Columns in Entity
                for column in Assetdata["entities"][0]["relationshipAttributes"]["columns"]:
                    columnGuid = column["guid"]
                    #print("column:",columnGuid)
                    #will compare against every referredEntity in the json
                    for relatedEntity in Assetdata["referredEntities"]:         
                        if relatedEntity == columnGuid:
                            #if they match we extract further info regarding the column
                            #print("entity:",relatedEntity)
                            #print("match")
                            current_column = Assetdata["referredEntities"][relatedEntity]
                            classificationNames = []
                            if "classifications" in current_column:
                                #print("Key exist in JSON data")                
                                #for each classification adds name to list
                                for classification in current_column["classifications"]:
                                    classificationNames.append(classification["typeName"])
                            else:
                                #print("Key does not exist in JSON data")
                                pass
                            #print(current_column["attributes"]["name"],":",current_column["attributes"]["data_type"])
                            
                            ColumnJsonDict = {
                            "name": current_column["attributes"]["name"],
                            "description": "",
                            "classifications": classificationNames,
                            "type": current_column["attributes"]["data_type"]
                            }
                            schemaList.append(ColumnJsonDict)
                #classifications in Entity
                if "classifications" in Assetdata["entities"][0]:
                    for classification in Assetdata["entities"][0]["classifications"]:
                        dataClassifications.append(classification["typeName"])
                #Check For Description
                if Assetdata["entities"][0]["attributes"]["userDescription"] is not None:
                    Desc = Assetdata["entities"][0]["attributes"]["userDescription"]
                    cleanDesc = remove_html_tags(Assetdata["entities"][0]["attributes"]["userDescription"])
                else:
                    cleanDesc = ""
                    Desc = ""
                #Split the FQN into a list for properties
                typePropertiesList = fqn.rsplit("/",4)
                testData ={
                            "key": guid,
                            "name": AssetName,
                            "description": Desc,
                            "descriptionStripped": cleanDesc,
                            "source": {
                                "type": "PurviewDataMap",
                                "fqn": fqn,
                                "accountName": accountName,
                                "assetId": guid,
                                "assetType": Assetdata["entities"][0]["typeName"]
                                },
                            "contacts": {
                                "owner": [],
                                "expert": [],
                                "databaseAdmin": []
                            },
                            "classifications": dataClassifications,
                            "schema": schemaList,
                            "lineage": lineageData,
                            "type": newDataType,
                            "typeProperties": {
                                "format": newDataFormat,
                                    "serverEndpoint": typePropertiesList[-4],
                                    "databaseName": typePropertiesList[-3],
                                    "schemaName": typePropertiesList[-2],
                                    "tableName": typePropertiesList[-1],
                                }
                            }
            ############
            #azure_datalake_gen2_resource_set
            ############
            case "azure_datalake_gen2_resource_set":
                print("azure_datalake_gen2_resource_set")
                newDataType = "ADLSGen2Path"
                newDataFormat = "File"
                schemaGuid = data["entities"][0]["relationshipAttributes"]["attachedSchema"][0]["guid"]
                url = f"https://{tenant_id}-api.purview-service.microsoft.com/catalog/api/atlas/v2/entity/bulk?excludeRelationshipTypes=dataset_process_inputs&excludeRelationshipTypes=process_dataset_outputs&excludeRelationshipTypes=process_parent&excludeRelationshipTypes=direct_lineage_dataset_dataset&guid={schemaGuid}&includeTermsInMinExtInfo=true&minExtInfo=false"
                response = requests.request("GET", url, headers=headers, data=payload)
                SchemaData = response.json()
                for relatedEntity in SchemaData["referredEntities"]:         
                    #if they match we extract further info regarding the column
                    #print("entity:",relatedEntity)
                    current_column = SchemaData["referredEntities"][relatedEntity]
                    classificationNames = []
                    #print(current_column.keys())
                    if "classifications" in current_column:
                        #print("Key 'classification' exist in JSON data")                
                        #for each classification adds name to list
                        for classification in current_column["classifications"]:
                            #print("classification:",classification["typeName"])
                            classificationNames.append(classification["typeName"])
                    else:
                        #print("Key 'classification' does not exist in JSON data")
                        pass
                    #print(current_column["attributes"]["name"],":",current_column["attributes"]["type"],":",current_column["attributes"]["userDescription"])
                    if current_column["attributes"]["userDescription"] is not None:
                        ColumnDesc = current_column["attributes"]["userDescription"]                                                      
                    else:
                        ColumnDesc = ""

                    ColumnJsonDict = {
                    "name": current_column["attributes"]["name"],
                    "description": ColumnDesc,
                    "classifications": classificationNames,
                    "type": current_column["attributes"]["type"]
                    }
                    schemaList.append(ColumnJsonDict)

                #classifications in Entity
                if "classifications" in Assetdata["entities"][0]:
                    for classification in Assetdata["entities"][0]["classifications"]:
                        dataClassifications.append(classification["typeName"])
                #Check For Description
                if Assetdata["entities"][0]["attributes"]["userDescription"] is not None:
                    Desc = Assetdata["entities"][0]["attributes"]["userDescription"]
                    cleanDesc = remove_html_tags(Assetdata["entities"][0]["attributes"]["userDescription"])
                else:
                    cleanDesc = ""
                    Desc = ""

                scheme= fqn.split("/")[0]
                base_url = fqn.split("/")[2]
                base_url = scheme+"//"+base_url+"/"
                container = fqn.split("/")[3]
                path = "/".join(fqn.split("/")[4:])
                path = path.split("{")[0]
                testData ={
                            "key": guid,
                            "name": AssetName,
                            "description": Desc,
                            "descriptionStripped": cleanDesc,
                            "source": {
                                "type": "PurviewDataMap",
                                "fqn": fqn,
                                "accountName": accountName,
                                "assetId": guid,
                                "assetType": Assetdata["entities"][0]["typeName"]
                                },
                            "contacts": {
                                "owner": [],
                                "expert": [],
                                "databaseAdmin": []
                            },
                            "classifications": dataClassifications,
                            "schema": schemaList,
                            "lineage": lineageData,
                            "type": newDataType,
                            "typeProperties": {
                                "serverEndpoint": base_url,
                                "container": container,
                                "folderPath": path
                            }
                        }
            ############
            #Unity Catalog Table
            ############

            case "databricks_table":        
                print("databricks_table")
                #Static Type
                newDataType = "General"
                newDataFormat = "Table"
                AssetName = simpleData["entities"][0]["attributes"]["name"]
                #Columns in Entity
                for column in Assetdata["entities"][0]["relationshipAttributes"]["columns"]:
                    columnGuid = column["guid"]
                    #print("column:",columnGuid)
                    #will compare against every referredEntity in the json
                    for relatedEntity in Assetdata["referredEntities"]:         
                        if relatedEntity == columnGuid:
                            #if they match we extract further info regarding the column
                            #print("entity:",relatedEntity)
                            #print("match")
                            current_column = Assetdata["referredEntities"][relatedEntity]
                            classificationNames = []
                            if "classifications" in current_column:
                                #print("Key exist in JSON data")                
                                #for each classification adds name to list
                                for classification in current_column["classifications"]:
                                    classificationNames.append(classification["typeName"])
                            else:
                                #print("Key does not exist in JSON data")
                                pass
                            #print(current_column["attributes"])
                            #print(current_column["attributes"]["name"],":",current_column["attributes"]["dataType"])
                            
                            ColumnJsonDict = {
                            "name": current_column["attributes"]["name"],
                            "description": "",
                            "classifications": classificationNames,
                            "type": current_column["attributes"]["dataType"]
                            }
                            schemaList.append(ColumnJsonDict)
                #classifications in Entity
                if "classifications" in Assetdata["entities"][0]:
                    for classification in Assetdata["entities"][0]["classifications"]:
                        dataClassifications.append(classification["typeName"])
                else:
                    dataClassifications = []

                #Check For Description
                if Assetdata["entities"][0]["attributes"]["userDescription"] is not None:
                    Desc = Assetdata["entities"][0]["attributes"]["userDescription"]
                    cleanDesc = remove_html_tags(Assetdata["entities"][0]["attributes"]["userDescription"])
                else:
                    cleanDesc = ""
                    Desc = ""
                                
                
                MetaStoreGUID = ""
                MetaStoreCheck= 0
                for Entity in lineageData["guidEntityMap"]:
                    if lineageData["guidEntityMap"][Entity]["typeName"] == "databricks_metastore":
                        MetaStoreGUID = lineageData["guidEntityMap"][Entity]["guid"]
                        MetaStoreCheck +=1
                if MetaStoreCheck == 0:
                    raise Exception("No Metastore Found")             

                url = f"https://{tenant_id}-api.purview-service.microsoft.com/catalog/api/atlas/v2/entity/bulk?excludeRelationshipTypes=databricks_metastore_catalogs&excludeRelationshipTypes=dataset_process_inputs&excludeRelationshipTypes=process_dataset_outputs&excludeRelationshipTypes=process_parent&excludeRelationshipTypes=direct_lineage_dataset_dataset&guid={MetaStoreGUID}&includeTermsInMinExtInfo=true&minExtInfo=true"
                response = requests.request("GET", url, headers=headers, data=payload)
                MetastoreData = response.json()


                DbURL = MetastoreData["entities"][0]["relationshipAttributes"]["workspaces"][0]["displayText"]
                testData =  {
                            "id": guid,
                            "name": AssetName,
                            "description": Desc,
                            "descriptionStripped": cleanDesc,
                            "source": {
                                "type": "PurviewDataMap",
                                "fqn": fqn,
                                "accountName": accountName,
                                "assetId": guid,
                                "assetType": Assetdata["entities"][0]["typeName"],
                                "AssetAttribute": {
                                    "workspaceUrl": DbURL
                                }
                                },
                            "contacts": {
                                "owner": [],
                                "expert": [],
                                "databaseAdmin": []
                            },
                            "classifications": dataClassifications,
                            "schema": schemaList,
                            "lineage": lineageData,
                            "type": newDataType,
                            "typeProperties": {}
                            }
            
            case "powerbi_report":
                #Static Type
                newDataType = "General"
                newDataFormat = "Table"
                AssetName = simpleData["entities"][0]["attributes"]["displayName"]
                if "classifications" in Assetdata["entities"][0]:
                    for classification in Assetdata["entities"][0]["classifications"]:
                        dataClassifications.append(classification["typeName"])
                else:
                    dataClassifications = []

                #Check For Description
                if Assetdata["entities"][0]["attributes"]["userDescription"] is not None:
                    Desc = Assetdata["entities"][0]["attributes"]["userDescription"]
                    cleanDesc = remove_html_tags(Assetdata["entities"][0]["attributes"]["userDescription"])
                else:
                    cleanDesc = ""
                    Desc = ""
                
                testData =  {
                                "key": guid,
                                "name": AssetName,
                                "description": Desc,
                                "descriptionStripped": cleanDesc,
                                "source": {
                                    "type": "PurviewDataMap",
                                    "fqn": fqn,
                                    "accountName": accountName,
                                    "assetId": guid,
                                    "assetType": Assetdata["entities"][0]["typeName"],
                                    },
                                "contacts": {
                                    "owner": [],
                                    "expert": [],
                                    "databaseAdmin": []
                                },
                                "classifications": dataClassifications,
                                "schema": [],
                                "lineage": lineageData,
                                "type": newDataType,
                                "typeProperties": {}
                                }


            
            
            case _:
                print("Asset Type ",AssetType," Not Supported. Skipping...")
                return None, None
            
        #create new asset
        url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/dataassets"
        headers = { "dataType": "json", "accept": "application/json", "Content-Type": "application/json", "Authorization": "Bearer " + access_token }
        payload = json.dumps(testData)
        response = requests.request("Post", url, headers=headers, data=payload)
        DataAssetData = response.json()

        newGuid = DataAssetData["id"]   
        description = DataAssetData["description"]

    return newGuid, description


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
