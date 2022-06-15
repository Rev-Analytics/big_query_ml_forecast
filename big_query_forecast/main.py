from flask import Flask, render_template, request
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
Simport os

credentials = service_account.Credentials.from_service_account_file(
'bq-service-keys.json')

project_id = 'u4cast-cr'
client = bigquery.Client(credentials= credentials,project=project_id)

app = Flask(__name__)

@app.route("/")
def index():

    model_build = """
            CREATE OR REPLACE MODEL `u4cast-cr.finance_data.armodel1`
            OPTIONS(MODEL_TYPE='ARIMA_PLUS',
                    time_series_timestamp_col='date',
                    time_series_data_col='sales',
                    time_series_id_col=['store_number','store_name', 'city','county']) AS
            SELECT
            date,
            store_number,
            store_name,
            city,
            county,
            sales
            FROM
            `u4cast-cr.finance_data.historical_sales` 
           """
    model_job = client.query(model_build) 

    fcast_n_load = """
        create or replace table `u4cast-cr.finance_data.forecast_load`
        as SELECT  * FROM   ML.FORECAST(MODEL `u4cast-cr.finance_data.armodel1`,
              STRUCT(26 AS horizon, 0.8 AS confidence_level))
            """
    fcast_job = client.query(fcast_n_load)

    text = 'completed !'

    return render_template('index.html', html_page_text=text)

if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 8080)))