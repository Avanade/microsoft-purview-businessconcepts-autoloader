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

%run GlossaryDetails

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def id_dict_list(id_list):
    dict_list = []
    for id in id_list:
        dict_list.append({"id":id})
    return dict_list

def build_hierarchy_iteratively(df, term):
    stack = [term]
    while stack:
        current_term = stack.pop()
        if current_term not in seen:
            seen.add(current_term)
            hierarchy_order.append(current_term)
            # Find the children of the current term and add them to the stack
            children = df[df["glossary_term_parent_term"] == current_term]["glossary_term_name"].tolist()
            stack.extend(children)    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import math
import pandas as pd
import numpy as np

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

glossary_terms =read_csv_from_adls_lakehouse("glossary_terms.csv")
glossary_terms = glossary_terms.sort_values(by='glossary_term_parent_term', ascending=False, na_position='first')
glossary_terms= glossary_terms.drop_duplicates(subset=['glossary_term_name',"business_domain_name"], keep='first')
#display(glossary_terms)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upload_glossary(csv_name,legacy=False,domain=""):
    if not legacy:
        glossary_terms =extract_csv_as_df_from_adls(csv_name)
        glossary_terms = glossary_terms.sort_values(by='glossary_term_parent_term', ascending=False, na_position='first')
        glossary_terms= glossary_terms.drop_duplicates(subset=['glossary_term_name',"business_domain_name"], keep='first')
        glossary_terms['glossary_term_owner'] = glossary_terms['glossary_term_owner'].apply(
        lambda user_email: get_user_id(user_email))
        glossary_terms['glossary_term_expert'] = glossary_terms['glossary_term_expert'].apply(
            lambda user_email: get_user_id(user_email))
        glossary_terms['glossary_term_database_admin'] = glossary_terms['glossary_term_database_admin'].apply(
            lambda user_email: get_user_id(user_email))

        glossary_terms['glossary_term_owner'] = glossary_terms['glossary_term_owner'].apply(id_dict_list)
        glossary_terms['glossary_term_expert'] = glossary_terms['glossary_term_expert'].apply(id_dict_list)
        glossary_terms['glossary_term_database_admin'] = glossary_terms['glossary_term_database_admin'].apply(id_dict_list)
        df = pd.DataFrame(glossary_terms)
        # Dictionary to store the hierarchical order
        hierarchy_order = []
        seen = set()  
        #Function to build the hierarchy iteratively


        # Add root terms (parent null)
        root_terms = df[df["glossary_term_parent_term"].isnull()]["glossary_term_name"].tolist()

        # Start the hierarchy with root terms
        for root in root_terms:
            if root not in seen:
                build_hierarchy_iteratively(df, root)

        # Create a dictionary for fast access to hierarchy indices
        index_map = {term: idx for idx, term in enumerate(hierarchy_order)}

        # Reorder the DataFrame according to the hierarchy
        df["hierarchy_order"] = df["glossary_term_name"].map(index_map)
        df_sorted = df.sort_values("hierarchy_order").drop(columns=["hierarchy_order"]).reset_index(drop=True)
        glossary_terms = df_sorted      
        df_sources=fetch_glossary()
        # Token and API URL setup
        access_token = get_token()
        url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/"
        headers = {
                "dataType": "json",
                "accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer " + access_token,
            }


        existing_domains = []
        created_domains = []

        for index, row in glossary_terms.iterrows():
            existing_entry = df_sources[
                (df_sources['name'] == row['glossary_term_name']) & 
                (df_sources['domain'] == row['glossary_term_business_domain_uuid'])
            ]
            
            if existing_entry.empty:

                payload = {
                        "name": row['glossary_term_name'],
                        "description": row['glossary_term_description'],  # Ensure description is properly formatted
                        "status": row['glossary_term_status'],
                        "contacts": {
                            "owner": row['glossary_term_owner'],
                            "expert": row['glossary_term_expert'],
                            "database_admin": row['glossary_term_database_admin'],
                        },
                        "acronyms": [row['glossary_term_acronyms']] if not pd.isna(row['glossary_term_acronyms']) else [],
                        "domain": row['glossary_term_business_domain_uuid']
                        
                }
                if pd.notna(row['glossary_term_parent_term']):
                    parent_id = get_glossary_id(row['glossary_term_parent_term'])
                    if parent_id:
                        payload["parentId"] = parent_id
                    else:
                        print(f"Warning: Parent term '{row['glossary_term_parent_term']}' not found for '{row['glossary_term_name']}'.")

                print(payload)    
                # Convert the payload to JSON
                payload_json = json.dumps(payload)

                # Make the POST request
                response = requests.post(url, headers=headers, data=payload_json)

                if response.status_code == 201:  # Successful creation
                    datasourceResponse = json.loads(response.text)
                    print(f"Successfully created glossary: {row['glossary_term_name']} business domain '{row['glossary_term_business_domain_uuid']}")
                    print(datasourceResponse)
                    created_domains.append(row['glossary_term_name'])
                elif response.status_code == 409:  # Conflict error (term already exists)
                    print(f"Glossary term '{row['glossary_term_name']}' already exists in domain '{row['glossary_term_business_domain_uuid']}'.")
                    existing_domains.append(row['glossary_term_name'])
                else:
                    print(f"Error creating glossary term: {row['glossary_term_name']} (status code: {response.status_code})")
                    print(response.text)
            else:
                print(f"Glossary term '{row['glossary_term_name']}' already exists in domain '{row['glossary_term_business_domain_uuid']}'.")
                existing_domains.append(row['glossary_term_name'])


        print("Finished processing glossary terms.")
        glossary_terms['glossary_term_name_uuid'] = glossary_terms['glossary_term_name'].apply(get_glossary_id)
        # Token and API URL setup
        #Synonyms#
        access_token = get_token()

        glossary_terms['glossary_term_name_uuid'] = glossary_terms['glossary_term_name'].apply(get_glossary_id)

        def create_contact_list(contact_list):
            return contact_list if not pd.isna(contact_list) else []

        existing_domains = []
        created_domains = []

        # Iterate over each row in glossary_terms DataFrame
        for index, row in glossary_terms.iterrows():
            glossary_term_name_uuid = row['glossary_term_name_uuid']  # Extract the unique ID
            glossary_term_name = row['glossary_term_name']
            url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/{glossary_term_name_uuid}/relationships?entityType=Term"

            # Construct the payload with only relevant updates
            payload = {
                "description": "",
                "entityId": row['glossary_term_synonyms_uuid'],
                "relationshipType": "Synonym"
            }
            # Convert the payload to JSON
            payload_json = json.dumps(payload)

            # Make the PUT request
            response = requests.post(url, headers=headers, data=payload_json)

            # Check the response
            if response.status_code == 200:  # Successful update
                print(f"Successfully updated glossary: {row['glossary_term_name_uuid']}")
                created_domains.append(row['glossary_term_name_uuid'])
            elif response.status_code == 409:  # Conflict error (term already exists)
                print(f"Glossary term '{row['glossary_term_name_uuid']}' already exists.")
                existing_domains.append(row['glossary_term_name_uuid']) 
            elif response.status_code == 400:  # Bad Request (validation error)
            # Check if the specific error message related to 'entityId' conversion is in the response text
                if "Error converting value" in response.text and "entityId" in response.text:
                    print(f"There is no value for '{row['glossary_term_name_uuid']}'")
                    existing_domains.append(row['glossary_term_name_uuid'])
                else:
                    print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: 400)")
                    print(response.text)
                
            else:
                print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: {response.status_code})")
                print(response.text)

        print("Finished processing glossary terms.")
        # Token and API URL setup
        #Related#
        access_token = get_token()

        def create_contact_list(contact_list):
            return contact_list if not pd.isna(contact_list) else []

        existing_domains = []
        created_domains = []

        # Iterate over each row in glossary_terms DataFrame
        for index, row in glossary_terms.iterrows():
            glossary_term_name_uuid = row['glossary_term_name_uuid']  # Extract the unique ID
            glossary_term_name = row['glossary_term_name']
            glossary_term_related_uuid = row['glossary_term_related_uuid']  # Extract the related term ID

            url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/{glossary_term_name_uuid}/relationships?entityType=Term"

            # Check if 'glossary_term_related_id' has a value before proceeding
            if pd.notna(glossary_term_related_uuid):
                # Construct the payload with only relevant updates
                payload = {
                    "description": "",
                    "entityId": glossary_term_related_uuid,
                    "relationshipType": "related" 
                }
                # Convert the payload to JSON
                payload_json = json.dumps(payload)

                # Make the POST request
                response = requests.post(url, headers=headers, data=payload_json)

                # Check the response
                if response.status_code == 200:  # Successful update
                    print(f"Successfully updated glossary: {row['glossary_term_name_uuid']}")
                    created_domains.append(row['glossary_term_name_uuid'])
                elif response.status_code == 400:  # Bad Request (validation error)
                    # Check if the specific error message related to 'entityId' conversion is in the response text
                    if "Error converting value" in response.text and "entityId" in response.text:
                        print(f"There is no value for '{row['glossary_term_name_uuid']}'")
                        existing_domains.append(row['glossary_term_name_uuid'])
                    else:
                        print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: 400)")
                        print(response.text)
                
            else:
                # Print a message if 'glossary_term_related_id' is missing
                print(f"Skipping glossary term '{glossary_term_name}' as no related ID is provided.")
                existing_domains.append(glossary_term_name)

        print("Finished processing glossary terms.")
        #data products
        data_products = fetch_data_products()
        glossary_terms = glossary_terms.merge(data_products[['name', 'id']],
                                            left_on='glossary_term_data_product',
                                            right_on='name',
                                            how='left')

        glossary_terms.rename(columns={'id': 'data_product_uuid'}, inplace=True)
        glossary_terms.drop(columns=['name'], inplace=True)
        glossary_terms.fillna("", inplace=True)
        # Token and API URL setup
        #Data Product Relation
        access_token = get_token()

        def create_contact_list(contact_list):
            return contact_list if not pd.isna(contact_list) else []

        existing_domains = []
        created_domains = []

        # Iterate over each row in glossary_terms DataFrame
        for index, row in glossary_terms.iterrows():
            glossary_term_name_id = row['glossary_term_name_uuid']  # Extract the unique ID
            glossary_term_name = row['glossary_term_name']
            data_product_uuid = row['data_product_uuid']  # Extract the related term ID

            url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/{glossary_term_name_uuid}/relationships?entityType=DataProduct"

            # Check if 'glossary_term_related_id' has a value before proceeding
            if pd.notna(glossary_term_related_uuid):
                # Construct the payload with only relevant updates
                payload = {
                    "description": "",
                    "entityId": data_product_uuid,
                    "relationshipType": "related" 
                }
                # Convert the payload to JSON
                payload_json = json.dumps(payload)

                # Make the POST request
                response = requests.post(url, headers=headers, data=payload_json)

                # Check the response
                if response.status_code == 200:  # Successful update
                    print(f"Successfully updated glossary: {row['glossary_term_name_uuid']}")
                    created_domains.append(row['glossary_term_name_uuid'])
                elif response.status_code == 400:  # Conflict error (term already exists)
                    print(f"Skipping gossary term '{glossary_term_name}'  as no data product ID is provided.")
                    existing_domains.append(glossary_term_name)      
                else:
                    print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: {response.status_code})")
                    print(response.text)
            else:
                # Print a message if 'glossary_term_related_id' is missing
                print(f"Skipping glossary term '{glossary_term_name}' as no data product is provided.")
                existing_domains.append(glossary_term_name)

        print("Finished processing glossary terms.")
                   
    
    else:
        pass    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(glossary_terms)

glossary_terms['glossary_term_business_domain_uuid'] = glossary_terms['business_domain_name'].apply(get_uuid)
term_business_domain_uuid = glossary_terms['glossary_term_business_domain_uuid']

#Added logic to get the guid of a glossary term for the business domain
glossary_terms['glossary_term_parent_term_uuid'] = glossary_terms.apply(
    lambda row: get_glossary_id(row['glossary_term_parent_term'], get_uuid(row['business_domain_name'])),
    axis=1
)

#glossary_terms['glossary_term_synonyms_uuid'] = glossary_terms['glossary_term_synonyms'].apply(get_glossary_id)
glossary_terms['glossary_term_synonyms_uuid'] = glossary_terms.apply(
    lambda row: get_glossary_id(row['glossary_term_synonyms'], get_uuid(row['glossary_term_synonyms_domain'])),
    axis=1
)

#glossary_terms['glossary_term_related_uuid'] = glossary_terms['glossary_term_related'].apply(get_glossary_id)
glossary_terms['glossary_term_related_uuid'] = glossary_terms.apply(
    lambda row: get_glossary_id(row['glossary_term_related'], get_uuid(row['glossary_term_related_domain'])),
    axis=1
)

display(glossary_terms)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

glossary_terms['glossary_term_owner'] = glossary_terms['glossary_term_owner'].apply(
    lambda user_email: get_user_id(user_email)
)
glossary_terms['glossary_term_expert'] = glossary_terms['glossary_term_expert'].apply(
    lambda user_email: get_user_id(user_email)
)
glossary_terms['glossary_term_database_admin'] = glossary_terms['glossary_term_database_admin'].apply(
    lambda user_email: get_user_id(user_email)
)

glossary_terms['glossary_term_owner'] = glossary_terms['glossary_term_owner'].apply(id_dict_list)
glossary_terms['glossary_term_expert'] = glossary_terms['glossary_term_expert'].apply(id_dict_list)
glossary_terms['glossary_term_database_admin'] = glossary_terms['glossary_term_database_admin'].apply(id_dict_list)
#display(glossary_terms)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.DataFrame(glossary_terms)
# Dictionary to store the hierarchical order
hierarchy_order = []
seen = set()  
#Function to build the hierarchy iteratively
def build_hierarchy_iteratively(df, term):
    stack = [term]
    while stack:
        current_term = stack.pop()
        if current_term not in seen:
            seen.add(current_term)
            hierarchy_order.append(current_term)
            # Find the children of the current term and add them to the stack
            children = df[df["glossary_term_parent_term"] == current_term]["glossary_term_name"].tolist()
            stack.extend(children)

# Add root terms (parent null)
root_terms = df[df["glossary_term_parent_term"].isnull()]["glossary_term_name"].tolist()

# Start the hierarchy with root terms
for root in root_terms:
    if root not in seen:
        build_hierarchy_iteratively(df, root)

# Create a dictionary for fast access to hierarchy indices
index_map = {term: idx for idx, term in enumerate(hierarchy_order)}

# Reorder the DataFrame according to the hierarchy
df["hierarchy_order"] = df["glossary_term_name"].map(index_map)
df_sorted = df.sort_values("hierarchy_order").drop(columns=["hierarchy_order"]).reset_index(drop=True)


glossary_terms = df_sorted
#display(glossary_terms)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sources=fetch_glossary()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Token and API URL setup
access_token = get_token()
url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/"
headers = {
        "dataType": "json",
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + access_token,
    }


existing_domains = []
created_domains = []

for index, row in glossary_terms.iterrows():
    existing_entry = df_sources[
        (df_sources['name'] == row['glossary_term_name']) & 
        (df_sources['domain'] == row['glossary_term_business_domain_uuid'])
    ]
    
    if existing_entry.empty:

        payload = {
                "name": row['glossary_term_name'],
                "description": row['glossary_term_description'],  # Ensure description is properly formatted
                "status": row['glossary_term_status'],
                "contacts": {
                    "owner": row['glossary_term_owner'],
                    "expert": row['glossary_term_expert'],
                    "database_admin": row['glossary_term_database_admin'],
                },
                "acronyms": [row['glossary_term_acronyms']] if not pd.isna(row['glossary_term_acronyms']) else [],
                "domain": row['glossary_term_business_domain_uuid']
                
        }
        if pd.notna(row['glossary_term_parent_term']):
            parent_id = get_glossary_id(row['glossary_term_parent_term'], row['glossary_term_business_domain_uuid'])
            if parent_id:
                payload["parentId"] = parent_id
            else:
                print(f"Warning: Parent term '{row['glossary_term_parent_term']}' not found for '{row['glossary_term_name']}'.")

        print(payload)    
        # Convert the payload to JSON
        payload_json = json.dumps(payload)

        # Make the POST request
        response = requests.post(url, headers=headers, data=payload_json)

        if response.status_code == 201:  # Successful creation
            datasourceResponse = json.loads(response.text)
            print(f"Successfully created glossary: {row['glossary_term_name']} business domain '{row['glossary_term_business_domain_uuid']}")
            print(datasourceResponse)
            created_domains.append(row['glossary_term_name'])
        elif response.status_code == 409:  # Conflict error (term already exists)
            print(f"Glossary term '{row['glossary_term_name']}' already exists in domain '{row['glossary_term_business_domain_uuid']}'.")
            existing_domains.append(row['glossary_term_name'])
        else:
            print(f"Error creating glossary term: {row['glossary_term_name']} (status code: {response.status_code})")
            print(response.text)
    else:
        print(f"Glossary term '{row['glossary_term_name']}' already exists in domain '{row['glossary_term_business_domain_uuid']}'.")
        existing_domains.append(row['glossary_term_name'])


print("Finished processing glossary terms.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(fetch_glossary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#glossary_terms['glossary_term_name_uuid'] = glossary_terms['glossary_term_name'].apply(get_glossary_id)

glossary_terms['glossary_term_name_uuid'] = glossary_terms.apply(
    lambda row: get_glossary_id(row['glossary_term_name'], get_uuid(row['business_domain_name'])),
    axis=1
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Token and API URL setup
access_token = get_token()

#glossary_terms['glossary_term_name_uuid'] = glossary_terms['glossary_term_name'].apply(get_glossary_id)

def create_contact_list(contact_list):
    return contact_list if not pd.isna(contact_list) else []

existing_domains = []
created_domains = []

# Iterate over each row in glossary_terms DataFrame
for index, row in glossary_terms.iterrows():
    glossary_term_name_uuid = row['glossary_term_name_uuid']  # Extract the unique ID
    glossary_term_name = row['glossary_term_name']
    url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/{glossary_term_name_uuid}/relationships?entityType=Term"

    # Construct the payload with only relevant updates
    payload = {
        "description": "",
        "entityId": row['glossary_term_synonyms_uuid'],
        "relationshipType": "Synonym"
    }
    # Convert the payload to JSON
    payload_json = json.dumps(payload)

    # Make the PUT request
    response = requests.post(url, headers=headers, data=payload_json)

    # Check the response
    if response.status_code == 200:  # Successful update
        print(f"Successfully updated glossary: {row['glossary_term_name_uuid']}")
        created_domains.append(row['glossary_term_name_uuid'])
    elif response.status_code == 409:  # Conflict error (term already exists)
        print(f"Glossary term '{row['glossary_term_name_uuid']}' already exists.")
        existing_domains.append(row['glossary_term_name_uuid']) 
    elif response.status_code == 400:  # Bad Request (validation error)
    # Check if the specific error message related to 'entityId' conversion is in the response text
        if "Error converting value" in response.text and "entityId" in response.text:
            print(f"There is no value for '{row['glossary_term_name_uuid']}'")
            existing_domains.append(row['glossary_term_name_uuid'])
        else:
            print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: 400)")
            print(response.text)
          
    else:
        print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: {response.status_code})")
        print(response.text)

print("Finished processing glossary terms.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Token and API URL setup
access_token = get_token()

def create_contact_list(contact_list):
    return contact_list if not pd.isna(contact_list) else []

existing_domains = []
created_domains = []
display(glossary_terms)
# Iterate over each row in glossary_terms DataFrame
for index, row in glossary_terms.iterrows():
    glossary_term_name_uuid = row['glossary_term_name_uuid']  # Extract the unique ID
    glossary_term_name = row['glossary_term_name']
    glossary_term_related_uuid = row['glossary_term_related_uuid']  # Extract the related term ID

    url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/{glossary_term_name_uuid}/relationships?entityType=Term"

    # Check if 'glossary_term_related_id' has a value before proceeding
    if pd.notna(glossary_term_related_uuid):
        # Construct the payload with only relevant updates
        payload = {
            "description": "",
            "entityId": glossary_term_related_uuid,
            "relationshipType": "related" 
        }
        # Convert the payload to JSON
        payload_json = json.dumps(payload)

        # Make the POST request
        response = requests.post(url, headers=headers, data=payload_json)

        # Check the response
        if response.status_code == 200:  # Successful update
            print(f"Successfully updated glossary: {row['glossary_term_name_uuid']}")
            created_domains.append(row['glossary_term_name_uuid'])
        elif response.status_code == 400:  # Bad Request (validation error)
            # Check if the specific error message related to 'entityId' conversion is in the response text
            if "Error converting value" in response.text and "entityId" in response.text:
                print(f"There is no value for '{row['glossary_term_name_uuid']}'")
                existing_domains.append(row['glossary_term_name_uuid'])
            else:
                print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: 400)")
                print(response.text)
        
    else:
        # Print a message if 'glossary_term_related_id' is missing
        print(f"Skipping glossary term '{glossary_term_name}' as no related ID is provided.")
        existing_domains.append(glossary_term_name)

print("Finished processing glossary terms.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#data products
data_products = fetch_data_products()


glossary_terms = glossary_terms.merge(data_products[['name', 'id']],
                                      left_on='glossary_term_data_product',
                                      right_on='name',
                                      how='left')

glossary_terms.rename(columns={'id': 'data_product_uuid'}, inplace=True)
glossary_terms.drop(columns=['name'], inplace=True)
glossary_terms.fillna("", inplace=True)

#display(glossary_terms)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Token and API URL setup
access_token = get_token()

def create_contact_list(contact_list):
    return contact_list if not pd.isna(contact_list) else []

existing_terms_dataprod = []
created_terms_dataprod = []

# Iterate over each row in glossary_terms DataFrame
for index, row in glossary_terms.iterrows():
    glossary_term_name_uuid = row['glossary_term_name_uuid']  # Extract the unique ID
    glossary_term_name = row['glossary_term_name']
    data_product_uuid = row['data_product_uuid']  # Extract the related term ID

    url = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog/terms/{glossary_term_name_uuid}/relationships?entityType=DataProduct"

    # Check if 'glossary_term_related_id' has a value before proceeding
    if pd.notna(data_product_uuid):
        # Construct the payload with only relevant updates
        payload = {
            "description": "",
            "entityId": data_product_uuid,
            "relationshipType": "related" 
        }
        # Convert the payload to JSON
        payload_json = json.dumps(payload)

        # Make the POST request
        response = requests.post(url, headers=headers, data=payload_json)

        # Check the response
        if response.status_code == 200:  # Successful update
            print(f"Successfully updated glossary: {row['glossary_term_name_uuid']}")
            created_terms_dataprod.append(row['glossary_term_name_uuid'])
        elif response.status_code == 400:  # Conflict error (term already exists)
            print(f"Skipping gossary term '{glossary_term_name}'  as no data product ID is provided.")
            existing_terms_dataprod.append(glossary_term_name)      
        else:
            print(f"Error updating glossary term: {row['glossary_term_name_uuid']} (status code: {response.status_code})")
            print(response.text)
    else:
        # Print a message if 'glossary_term_related_id' is missing
        print(f"Skipping glossary term '{glossary_term_name}' as no data product is provided.")
        existing_terms_dataprod.append(glossary_term_name)

print("Finished processing glossary terms.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
