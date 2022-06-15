# u4cast_flask
[![Python application test with Github Actions](https://github.com/Rev-Analytics/u4cast_flask/actions/workflows/testing_ci.yml/badge.svg)](https://github.com/Rev-Analytics/u4cast_flask/actions/workflows/testing_ci.yml)


### Code kicks off the BigQuery Forecasting process<br>

1. First it takes in csv file from user. This represents the periodic (in this case weekly) actuals data.
2. The code pushes that file to a cloud bucket that then triggers a cloud function. 
3. The cloud function (not in this repo) will send the data to big query table
