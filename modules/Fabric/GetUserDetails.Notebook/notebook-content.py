# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests

def get_user_id(user_email):
    service_principal_id = "ec7313b1-dc75-44d6-a248-1412d06c9f55"
    service_principal_secret_name = "45L8Q~B_8t36C~LYhtUDoz6BPDG8D8Uc-4qCqaBc"
    tenant_id = "467750fa-44ec-49d4-ba43-5ae49d501676"

    # Token endpoint
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    # Request parameters
    params = {
        'grant_type': 'client_credentials',
        'client_id': service_principal_id,
        'client_secret': service_principal_secret_name,
        'scope': 'https://graph.microsoft.com/.default'
    }

    # Make the POST request
    response = requests.post(url, data=params)

    # Check if the request was successful
    if response.status_code == 200:
        access_token = response.json().get('access_token')
    else:
        print(f'Error: {response.status_code}')
        print(response.json())
        return None

    # Set the headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    list_email = user_email.split(",")
    list_email_id =[]
    
    for user_email in list_email:
        # Graph API endpoint to get user details
        url = f'https://graph.microsoft.com/v1.0/users?$filter=mail eq \'{user_email}\''

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            user_data = response.json()
            if user_data['value']:
                user_id = user_data['value'][0]['id']
                list_email_id.append(user_id)
            else:
                print('User not found.')
                pass
        else:
            print(f'Error: {response.status_code}')
            print(response.json())
            pass
        
    return list_email_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
