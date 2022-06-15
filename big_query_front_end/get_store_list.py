
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# variables 
gcp_project = 'u4cast-cr'
bq_dataset = 'finance_data'

"""
used to get the list of stores directly from big query
"""

#key_path = '/workspaces/u4cast_flask/u4cast-cr-948010851c7c.json'
key_path = '/workspaces/u4cast_flask/cloud_storage_key.json'

credentials = service_account.Credentials.from_service_account_file(
     key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"], )


client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


get_stores =  """
    select distinct store_number, store_name from (

      select store_number,
          store_name,
         row_number() over (partition by lower(store_number) order by store_name) as rn 
    FROM `u4cast-cr.finance_data.historical_sales`
    ) t
    where rn = 1; """

stores = client.query(get_stores).result()

stores_df = stores.to_dataframe()

print('stores head',stores_df.head())

stores_df.to_csv('stores.csv',index=False)