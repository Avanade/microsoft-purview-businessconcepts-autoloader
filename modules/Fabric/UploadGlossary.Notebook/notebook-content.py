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
# META     "environment": {
# META       "environmentId": "e6f73cb2-325b-4dcc-a007-b1824af8dfaf",
# META       "workspaceId": "cf8bb0d9-a61b-4a31-9f6a-37474df4f23a"
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

tenant_id = "467750fa-44ec-49d4-ba43-5ae49d501676"
access_token = get_token()
url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/terms/"
payload = {}
headers = {
    "dataType": "json",
    "accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Bearer " + access_token
}
records = []

response = session.get(url, headers=headers, data=payload, stream=True)
datasourceResponse = response.json()
for row in datasourceResponse.get("value", []):
    records.append([
        row.get("id"),
        row.get("name"),
        row.get("domain"),
        row.get("description"),
        json.dumps(row.get("contacts")),
        row.get("status")
    ])

column_names = ["ID", "Name", "Domain","Description", "Contacts", "Status"]
df_sources = pd.DataFrame(records, columns=column_names)

display(df_sources)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

glossary_terms =read_csv_from_adls_lakehouse("glossary_terms.csv")
glossary_terms = glossary_terms.sort_values(by='glossary_term_parent_term', ascending=False, na_position='last')
display(glossary_terms)

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

# Helper function to sort glossary terms hierarchically
def sort_hierarchically(df):
    # Separate rows with and without parent terms
    no_parent_df = df[df['glossary_term_parent_term'].isna()].copy()
    with_parent_df = df[~df['glossary_term_parent_term'].isna()].copy()

    # Initialize an empty list to store the ordered terms
    ordered_terms = []

    # Iterate through the terms without parents first
    for _, row in no_parent_df.iterrows():
        ordered_terms.append(row['glossary_term_name'])

        # For each term without a parent, find its children and add them
        children = with_parent_df[with_parent_df['glossary_term_parent_term'] == row['glossary_term_name']]
        while not children.empty:
            for _, child_row in children.iterrows():
                ordered_terms.append(child_row['glossary_term_name'])
                # Find grandchildren (if any)
                grandchildren = with_parent_df[with_parent_df['glossary_term_parent_term'] == child_row['glossary_term_name']]
                children = grandchildren

    # Create a sorted DataFrame based on the order of terms
    ordered_df = pd.DataFrame(ordered_terms, columns=['glossary_term_name'])
    return df.set_index('glossary_term_name').loc[ordered_df['glossary_term_name']].reset_index()

# Apply the hierarchical sorting
sorted_glossary_terms = sort_hierarchically(glossary_terms)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten the column 'glossary_term_business_domain'
# This ensures we extract the actual values from the Series within Series
glossary_terms['glossary_term_business_domain_flat'] = glossary_terms['business_domain_name'].apply(lambda x: x.iloc[0] if isinstance(x, pd.Series) else x)

# Function to apply get_uuid() to each value in the 'glossary_term_business_domain_flat' column
def apply_get_uuid(domain_name):
    if isinstance(domain_name, pd.Series) or isinstance(domain_name, list):
        print(f"Error: {domain_name} is not a string, it's a {type(domain_name)}")  # Debugging
        return None  # Handle cases where the value is not a string
    if pd.isna(domain_name):  # Handle NaN values
        return None
    return get_uuid(domain_name)

# Apply the function to the flattened column
glossary_terms['glossary_term_business_domain_uuid'] = glossary_terms['glossary_term_business_domain_flat'].apply(apply_get_uuid)
glossary_terms.drop(columns=['glossary_term_business_domain_flat'], inplace=True)

# Print the updated DataFrame to check the new column with UUIDs
#print(glossary_terms[['glossary_term_business_domain_uuid']])


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
#display(glossary_terms)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

glossary_terms['glossary_term_owner'] = glossary_terms['glossary_term_owner'].apply(lambda x: [{'ID': x}])
glossary_terms['glossary_term_expert'] = glossary_terms['glossary_term_expert'].apply(lambda x: [{'ID': x}])
glossary_terms['glossary_term_database_admin'] = glossary_terms['glossary_term_database_admin'].apply(lambda x: [{'ID': x}])
display(glossary_terms)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

glossary_terms

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the mapping for column pairs to merge and rename, now including 'glossary_term_related'
columns_to_merge = {
    'glossary_term_parent_term': 'glossary_term_parent_term_id',
    'glossary_term_synonyms': 'glossary_term_synonyms_id',
    'glossary_term_related': 'glossary_term_related_id'  # New column to map related terms
}

df_sources["Name"] = df_sources["Name"].astype("string")


# Merge for 'glossary_term_name' without removing it
glossary_terms = glossary_terms.merge(df_sources[['Name', 'ID']],
                                      left_on='glossary_term_name',
                                      right_on='Name',
                                      how='left',
                                      suffixes=('_x1', '_x2'))

# Rename the 'ID' column to 'glossary_term_name_id' but keep 'glossary_term_name'
glossary_terms.rename(columns={'ID': 'glossary_term_name_id'}, inplace=True)
glossary_terms.drop(columns=['Name'], inplace=True)  # Drop only 'Name', not 'glossary_term_name'

# Iterate over the other columns that need merging and renaming
for col, new_col in columns_to_merge.items():
    # Merge with df_sources on the specific column
    glossary_terms = glossary_terms.merge(df_sources,
                                          left_on=col,
                                          right_on='Name',
                                          how='left',
                                          suffixes=('_y1', '_y2'))

    # Rename the 'ID' column to the new name and drop unnecessary columns
    glossary_terms.rename(columns={'ID': new_col}, inplace=True)
    glossary_terms.drop(columns=['Name', col], inplace=True)

# Optionally, sort based on 'glossary_term_parent_term_id'
glossary_terms = glossary_terms.sort_values(by='glossary_term_parent_term_id', ascending=True, na_position='last')
glossary_terms.fillna("", inplace=True)

glossary_terms = glossary_terms.drop_duplicates(subset=["glossary_term_name"])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

glossary_terms

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Token and API URL setup
access_token = get_token()
url = f"https://{tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog/terms/"

def create_contact_list(contact_list):
    if pd.isna(contact_list):
        return []
    return contact_list  # Now we assume the column is already in the correct format

existing_domains = []
created_domains = []

# Iterate over each row in glossary_terms DataFrame
for index, row in glossary_terms.iterrows():
    # Check if the glossary_term_name exists in the same domain in df_sources
    existing_entry = df_sources[
        (df_sources['Name'] == row['glossary_term_name']) & 
        (df_sources['Domain'] == row['glossary_term_business_domain_uuid'])
    ]
    
    # Proceed if the glossary term doesn't exist in the same domain
    if existing_entry.empty:
        
        # Construct the payload
        payload = {
            "name": row['glossary_term_name'],
            "description": row['glossary_term_description'],  # Ensure description is properly formatted
            "status": row['glossary_term_status'],
            "contacts": {
                "owner": create_contact_list(row['glossary_term_owner']),
                "expert": create_contact_list(row['glossary_term_expert']),
                "database_admin": create_contact_list(row['glossary_term_database_admin']),
            },
            "acronyms": [row['glossary_term_acronyms']] if not pd.isna(row['glossary_term_acronyms']) else [],
            "domain": row['glossary_term_business_domain_uuid'],
            "parentId": row['glossary_term_parent_term_id'] 
        }
        
        # Convert the payload to JSON
        payload_json = json.dumps(payload)

        # Make the POST request
        response = requests.post(url, headers=headers, data=payload_json)

        # Check the response
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
