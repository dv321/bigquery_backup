Efficiently backs up bigquery data to cloud storage.

Install the requirements:
```pip install -r requirements.txt```

Then add your google project private key and service account email to auth_utils.py

To run the script invoke bigquery.py with the project name and dataset name as the arguments.

Note that this is an example project created to demo how one could efficiently copy data out of bigquery; in production there are bigquery export jobs available to do the same thing much faster.
