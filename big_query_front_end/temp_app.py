from flask import Flask, render_template, request
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

import os
credentials = service_account.Credentials.from_service_account_file(
'u4cast-cr-948010851c7c.json')

project_id = 'u4cast-cr'
client = bigquery.Client(credentials= credentials,project=project_id)

app = Flask(__name__)

# SECRET_KEY = os.urandom(32)
# app.config['SECRET_KEY'] = SECRET_KEY

@app.route("/")
def index():
    df=pd.read_csv("stores.csv")

    stores = list (df.values)
    #print(stores)
    return render_template('example.html', stores=stores)

@app.route("/test", methods=['GET','POST'])
def test():
    """
    pass in select variable through to the query 
    """

    select = request.form.get('select')
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("store_number", "STRING", select),
            ]
             )

    # pull data from archive/historical sales table
    pull_history = '''
     select store_number, date, sales from `u4cast-cr.finance_data.historical_sales` where store_number = @store_number
     '''
    query_job = client.query(pull_history, job_config=job_config)  # Make an API request.

    q_results =query_job.result()
    hist_df = q_results.to_dataframe()
    hist_df['source'] = 'historical'
    print('history',hist_df.head())

    # pull data from forecast load table    
    forecast = '''
     SELECT store_number, forecast_timestamp as date, forecast_value as sales FROM `u4cast-cr.finance_data.forecast_load` where store_number = @store_number
     '''
    query_job_fc = client.query(forecast, job_config=job_config)  # Make an API request.

    q_results_fc =query_job_fc.result()
    fc_df = q_results_fc.to_dataframe()
    fc_df['source'] = 'forecast'
#    fc_df.date = fc_df.date.dt.date 
    fc_df.date= pd.to_datetime(fc_df.date).dt.date

    # print('history dtypes',hist_df.dtypes)

    # print('forecast dytpes',fc_df.dtypes)



    # # filter forecast data set for dates beyond history
    # # so there is no overlap in dates
    fc_df = fc_df[fc_df.date > hist_df.date.max()]

    #print('forecast',fc_df.head())

    # concatenate the two dataframes
    bq_df = pd.concat([hist_df,fc_df])

    print(bq_df.head())
    bq_df.to_csv(f'{select}_data_file.csv')
    return(str(select))


if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 8080)))    


