import json
from flask import (
    Flask,
    make_response,
    render_template, 
    redirect,
    request, 
    url_for, 
    send_file)
from flask_wtf import FlaskForm
#from subprocess import Popen, PIPE
import os
#from os import listdir
from os.path import isfile, join


from google.cloud import storage
from create_config import bucketName

from google.cloud import bigquery
from google.oauth2 import service_account
#, localFolder
#, bucketFolder

from wtforms import StringField, IntegerField,  DateTimeField

from wtforms.validators import DataRequired
import pandas as pd

# main file tha that uploads the csv file to push to bigquery
credentials = service_account.Credentials.from_service_account_file('bigquery_service_key.json')

project_id = 'u4cast-cr'
client = bigquery.Client(credentials= credentials,project=project_id)

# create a variable to show which files are accepted in upload
ALLOWED_EXTENSIONS = {"csv","xlsx"}

app = Flask(__name__)

SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'bigquery_service_key.json'

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1] in ALLOWED_EXTENSIONS

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucketName)

@app.route("/",methods=['GET','POST'])
def main():
      # add in code for get max date
    as_of_date = get_date()
    print(as_of_date)
    return render_template('main.html',latestdate=as_of_date)#,as_of_date = as_of_date)

@app.route("/index", methods=['GET','POST'] )
def index():
    if request.method == 'POST':
        file = request.files['csvfile']
            # check uploaded file
        if not file:
            return "No file selected"
        if not allowed_file(file.filename):
            return "The file format is not allowed. Please upload a csv"
        if not os.path.isdir('static'):
            os.mkdir('static')
        else:
            print('no file created')
        csv_name = 'static/'+file.filename 
        filepath = os.path.join(csv_name)

        file.save(filepath)

        new_filename = file.filename.replace('.csv','.json')
        json_name = 'static/'+new_filename
        filepath_json = os.path.join(json_name)
       
        # read in the same exact file
        df_csv = pd.read_csv(csv_name)

        df_csv.to_json(filepath_json)
        #print('bucketfolder',bucketFolder)

        os.remove(csv_name)

        blob = bucket.blob(new_filename)

        blob.upload_from_filename(filepath_json)

        # check that local file made it to the bucket
        files = bucket.list_blobs()
        print('files',files)
        fileList = [file.name for file in files if '.' in file.name]
        # delete local file after check
        print('bucket filelist',fileList)
        if file.filename in fileList:
            os.remove(filepath)   
            print(f'{file.filename} deleted from bucket.')

        print('next move to example html')
#        return render_template(example)
        return redirect(url_for('example'))
    print('not exiting yet')
    return render_template('index.html')

@app.route("/example")
def example():
    df=pd.read_csv("stores.csv")

    stores = list(df.values)
    print('stores',stores[:10])
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
    hist_df['date'] = pd.to_datetime(hist_df['date'])
    # for debugging purposes
    print('history dtypes',hist_df.dtypes)


    # pull data from forecast load table    
    forecast = '''
     SELECT store_number, forecast_timestamp as date, forecast_value as sales FROM `u4cast-cr.finance_data.forecast_load` where store_number = @store_number
     '''
    query_job_fc = client.query(forecast, job_config=job_config)  # Make an API request.

    q_results_fc =query_job_fc.result()
    fc_df = q_results_fc.to_dataframe()
    fc_df['source'] = 'forecast'
    # may need to change this
    fc_df['date']= pd.to_datetime(fc_df['date']).dt.date
    fc_df['date'] = pd.to_datetime(fc_df['date'])
    # for debugging purposes
    print('forecast dtypes',fc_df.dtypes)


    fc_df = fc_df[fc_df.date > hist_df.date.max()]
    bq_df = pd.concat([hist_df,fc_df])

    bq_df.sort_values(['date'],inplace=True)
    bq_df.index = range(len(bq_df))

    #print(bq_df.head())
    
    #bq_df.reset_index(drop=True, inplace=True)
    #print('columns for bq_df',bq_df.columns)
    # bq_df.to_csv(f'{select}_data_file.csv')
    #bq_df.date = pd.to_datetime(bq_df['date']).to_numpy()
    bq_json = bq_df.to_json()
    bq_json = json.loads(bq_json)
    # print(bq_json['date'])
    fr_len = len(fc_df)
    print(fr_len)
    # add as of data and pass this as a parameter
    # resp = make_response(render_template('bq_table.html'))
    
    return render_template("bq_table.html",bq_forecast_length=fr_len, bq_row_data=bq_json,zip=zip)
    # return(resp)

def get_date():
    get_max_date = """
                SELECT max(date) FROM `u4cast-cr.finance_data.historical_sales`
                """
    query_job = client.query(get_max_date)  # Make an API request.
    for row in query_job:
      max_date = row[0]
    return(max_date)


if __name__ == "__main__":
    app.run(debug=False, port=int(os.environ.get("PORT", 8080)))   
