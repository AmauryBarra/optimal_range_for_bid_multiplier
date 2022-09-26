import os

# Google cloud service accout
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT", "")

# Bigquery configuration
BIGQUERY_PROJECT = "martin-test-datalab"
BIGQUERY_LOCATION = "EU"