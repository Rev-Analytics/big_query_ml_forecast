# Streaming data from Cloud Storage into BigQuery using Cloud Functions
Started with code provided by GCP:
https://github.com/GoogleCloudPlatform/solutions-gcs-bq-streaming-functions-python

### Modified to take in data, then flush out incoming cloud storage bucket

After file is converted to json and placed in target bucket, data is sent to Bigquery. Success message is sent to pub/sub. The success msg triggers staging folder to be cleard.
