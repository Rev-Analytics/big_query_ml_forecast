
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# variables 
gcp_project = 'u4cast-cr'
bq_dataset = 'finance_data'

# connections

# TODO(developer): Set key_path to the path to the service account key
#                  file.
#key_path = "/workspaces/u4cast_flask/u4cast-cr-f986975708b5.json"
key_path = '/workspaces/u4cast_flask/u4cast-cr-948010851c7c.json'

credentials = service_account.Credentials.from_service_account_file(
     key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"], )


client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

print('client',type(client))

#client = bigquery.Client(project=gcp_project)

# client = bigquery.Client.from_service_account_json(
#         'u4cast-cr-f986975708b5.json',project=gcp_project)


#client = bigquery.Client(project='u4cast')
#dataset_ref = client.dataset(bq_dataset)




# history = client.query('''
#  SELECT 
#  date,
#  sum(sales) as sales

#   FROM `u4cast-cr.finance_data.historical_sales` 

#   group by 1

# ''').to_dataframe()

# print(f'history {history.head()}')

query_test = '''
SELECT * FROM `u4cast-cr.finance_data.historical_sales` LIMIT 10
'''

# df = client.query(query_test).results().to_dataframe()

query_object = client.query(query_test)

print('type of query object',type(query_object))

q_results =query_object.result()

print('q_results',type(q_results))

df = q_results.to_dataframe()

print('incoming dataframe',df.head())

#print('type of q object results', type(query_object.results()))

# print(f'this is the data frame {df.head()}')


# # Read what ever data is in 'input file' and create a model
# create_model = '''
# CREATE OR REPLACE MODEL `u4cast.ds_input.armodel1`
#  OPTIONS(MODEL_TYPE='ARIMA_PLUS',
#          time_series_timestamp_col='date',
#          time_series_data_col='total_amount_sold',
#          time_series_id_col=['county', 'category']) AS
# SELECT
#   date,
#   total_amount_sold,
#   county,
#   category
# FROM
# `u4cast.ds_input.input_file` 
# '''

# run_model = '''
# create or replace table `u4cast.ds_input.forecast`
# as SELECT
#   *
# FROM
#   ML.FORECAST(MODEL `ds_input.armodel1`,
#               STRUCT(30 AS horizon, 0.8 AS confidence_level))
# '''

# pull_forecast = '''
# SELECT * from `u4cast.ds_input.forecast`
# '''


# # train data and save model
# client.query(create_model)
# # run predictions
# client.query(run_model)
# # pull data
# query = client.query(pull_forecast)

# out_data = query.result()

# df = out_data.to_dataframe()

# print(df.head())