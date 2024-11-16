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

tenant_id="467750fa-44ec-49d4-ba43-5ae49d501676"
business_domains_df = read_csv_from_adls_lakehouse("business_domains.csv")
#display(business_domains_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sources_pd = fetch_business_domains()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df_sources_pd
column_values_list = business_domains_df["business_domain_parent"].tolist()
print(column_values_list)
guid_list = []
specific_data_list = []
specific_data = pd.DataFrame()

for item in column_values_list:
    row = df.loc[df["name"] == item]
    
    if not row.empty:
        guid = row["id"].iloc[0]  
        guid_list.append(guid)
        specific_data_list.append(row)  
    else:
        guid_list.append(None)  

if specific_data_list:
    specific_data = pd.concat(specific_data_list)
if "business_domain_parent_guid" not in business_domains_df.columns:
    business_domains_df.insert(len(business_domains_df.columns), "business_domain_parent_guid", guid_list, True)

#display(business_domains_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Efficiently check for existing names using sets
existing_names = business_domains_df["business_domain_name"]

access_token = get_token()
url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/businessdomains"

existing_domains = []
created_domains =[]
for index, row in business_domains_df.iterrows():
    if row['business_domain_name'] not in df_sources_pd['name'].values:
        payload = {
            "name": row['business_domain_name'],
            "owners": row['business_domain_email_owners'],
            "status": row['business_domain_status'],
            "type": row['business_domain_type'],
            "description": row['business_domain_description'],
            "parentId": row['business_domain_parent_guid'],
        }
        
        payload_json = json.dumps(payload)

        headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + access_token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"
        }
        
        response = requests.post(url, headers=headers, data=payload_json)

        if response.status_code == 201:  # Handle successful creation
            datasourceResponse = json.loads(response.text)
            print(f"Successfully created business domain: {row['business_domain_name']}")
            print(datasourceResponse)
            created_domains.append(row['business_domain_name'])
        elif response.status_code == 409:  # Handle conflict error (domain already exists)
            print(f"Business domain '{row['business_domain_name']}' already exists.")
            existing_domains.append(row['business_domain_name'])
        else:
            print(f"Error creating business domain: {row['business_domain_name']} (status code: {response.status_code})")
            print(response.text)
    else:
        print(f"Business domain '{row['business_domain_name']}' already exists.")
        existing_domains.append(row['business_domain_name'])

if existing_domains:
    print("The following business domains already exist:")
    for domain in existing_domains:
        print(f"- {domain}")

print("Finished processing business domains.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sources_pd = fetch_business_domains()
df_joined = pd.merge(df_sources_pd, business_domains_df, left_on='name', right_on='business_domain_name')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_joined

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # add policies

# CELL ********************

access_token = get_token()
url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/policies"
payload = {}
headers = { "dataType": "json", "accept": "application/json", "Content-Type": "application/json", "Authorization": "Bearer " + access_token }
records = []

#getting jsonResponse from RESTAPI method
response = session.get(url, headers=headers, data=payload, stream=True)
datasourceResponse = json.loads(json.dumps(response.json(), indent=4))
#display(datasourceResponse)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_token()
# For Loop that will go thru the Joined Dataframe and use the BD ID at each and add the emails to the policy 
for index, row in df_joined.iterrows():
    dgpolicy_businessdomain = "dgpolicy_businessdomain_" + row["id"]
    #print(dgpolicy_businessdomain)
    dgpolicy_dgdataqualityscope = "dgpolicy_dgdataqualityscope_" + row["id"]
    #print(dgpolicy_dgdataqualityscope)
    dgpolicy_steward = dgpolicy_dgdataqualityscope
    # Adding the User IDs on the DG Policy Quality Scope
    for index, item in enumerate(datasourceResponse['values']):
        if item['name'] == dgpolicy_dgdataqualityscope:
            index_target_dq = index
            DQPolicyID = datasourceResponse['values'][index_target_dq]["id"]
            url = f"https://{tenant_id}-api.purview-service.microsoft.com/policystore/datagovernancePolicies/{DQPolicyID}?api-version=2023-06-01-preview"
            payload = {}
            headers = {
                "dataType": "json",
                "accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer " + access_token
            }
            response = session.get(url, headers=headers, json=payload)
            policyResponse = json.loads(json.dumps(response.json()))
            #print(policyResponse)
            DataGovAdminPolicyIDs = ""
            # Check if 'properties' exists in the response
            if 'properties' in policyResponse:
                DataGovAdminPolicyEmails = row['business_domain_email_owners'].replace(" ", "")
                DataGovAdminPolicyIDs = get_user_id(DataGovAdminPolicyEmails)
                #print("DataGovAdminPolicyIDs ", DataGovAdminPolicyIDs)
                builtin_scope_path = policyResponse['properties']['attributeRules'][0]['dnfCondition'][0][0]['attributeValueIncludedIn']
                #print("included policy ids: ", builtin_scope_path)
                for policy_id in DataGovAdminPolicyIDs:
                    #print("true")
                    if policy_id not in builtin_scope_path:
                        #print("true 2")
                        builtin_scope_path.append(policy_id)
                        #print("policy response ", policyResponse['properties']['attributeRules'][0]['dnfCondition'][0][0]['attributeValueIncludedIn'])
                        #print("new response ",builtin_scope_path)
                url = f"https://{tenant_id}-api.purview-service.microsoft.com/policystore/datagovernancePolicies/{DQPolicyID}?api-version=2023-06-01-preview"
                payload = policyResponse
                headers = {
                    "dataType": "json",
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + access_token
                }
                #print(payload)
                response = session.put(url, headers=headers, json=payload)
                print("Policy Quality status code ", response.status_code)
                policyResponse = json.loads(json.dumps(response.json()))

        # Adding the User IDs on the BD policy Scope
        if item['name'] == dgpolicy_businessdomain:
            index_target_bd = index
            DataGovAdminPolicyID = datasourceResponse['values'][index_target_bd]["id"]
            url = f"https://{tenant_id}-api.purview-service.microsoft.com/policystore/datagovernancePolicies/{DataGovAdminPolicyID}?api-version=2023-06-01-preview"
            payload = {}
            headers = {
                "dataType": "json",
                "accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer " + access_token
            }
            response = session.get(url, headers=headers, json=payload)
            policyResponse = json.loads(json.dumps(response.json()))
            DataGovAdminPolicyIDs = ""
            # Check if 'properties' exists in the response
            if 'properties' in policyResponse:
                dgpolicy_businessdomain_path = policyResponse['properties']["attributeRules"][0]["dnfCondition"][0][0]["attributeValueIncludedIn"]
                DataGovAdminPolicyEmails = row['business_domain_email_owners'].replace(" ", "")
                DataGovAdminPolicyIDs = get_user_id(DataGovAdminPolicyEmails)
                #print("user ids: ",DataGovAdminPolicyIDs)
                for policy_id in DataGovAdminPolicyIDs:
                    if policy_id not in dgpolicy_businessdomain_path:
                        #print("true")
                        dgpolicy_businessdomain_path.append(policy_id)
                        url = f"https://{tenant_id}-api.purview-service.microsoft.com/policystore/datagovernancePolicies/{DataGovAdminPolicyID}?api-version=2023-06-01-preview"
                        payload = policyResponse
                        headers = {
                            "dataType": "json",
                            "accept": "application/json",
                            "Content-Type": "application/json",
                            "Authorization": "Bearer " + access_token
                        }
                        response = session.put(url, headers=headers, json=payload)
                        print("DG policy Scope status code ", response.status_code)
                        policyResponse = json.loads(json.dumps(response.json()))
                        #print(policyResponse)
        # Adding the User IDs on the BD steward    
        if item['name'] == dgpolicy_steward:
            index_target_dq = index
            DataGovAdminPolicyID = datasourceResponse['values'][index_target_dq]["id"]
            url = f"https://{tenant_id}-api.purview-service.microsoft.com/policystore/datagovernancePolicies/{DataGovAdminPolicyID}?api-version=2023-06-01-preview"
            payload = {}
            headers = {
                "dataType": "json",
                "accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer " + access_token
            }
            response = session.get(url, headers=headers, json=payload)
            policyResponse = json.loads(json.dumps(response.json()))
            # Check if 'properties' exists in the response
            if 'properties' in policyResponse:
                target_sterward_path = policyResponse['properties']["attributeRules"][2]["dnfCondition"][0][0]["attributeValueIncludedIn"]
                for policy_id in DataGovAdminPolicyIDs:
                    if policy_id not in target_sterward_path:
                        target_sterward_path.append(policy_id)
                url = f"https://{tenant_id}-api.purview-service.microsoft.com/policystore/datagovernancePolicies/{DataGovAdminPolicyID}?api-version=2023-06-01-preview"
                payload = policyResponse
                headers = {
                    "dataType": "json",
                    "accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + access_token
                }
                response = session.put(url, headers=headers, json=payload)
                print("steward status code ", response.status_code)
                policyResponse = json.loads(json.dumps(response.json()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
