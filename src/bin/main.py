import requests
import os
import logging
import logging.config
#from local_to_gcs import ingestion, create_bucket, upload_blob, delete_locale_file
from local_to_gcs import ingestion, create_bucket, upload_blob


#log_file_abs = os.path.abspath("C:\\Users\\WTA23811\\etl-gcp\\src\\util\\logging_to_file.conf")
#logging.config.fileConfig(fname=log_file_abs)
#logger = logging.getLogger(__name__)

url = 'http://api.coincap.io/v2/assets'
file_name = "file_csv.csv"
bucket_name = "my-data-54"
location = "us-central1"
def main():
    data_json = ingestion(
        url_input=url
    )

    create_bucket(
        bucket_name=bucket_name,
        location=location
    )

    upload_blob(
        bucket_name=bucket_name,
        source_file_name=file_name,
        destination_blob_name=file_name
    )
"""""
    delete_locale_file(
        file_path=file_name
    )
"""

if __name__=="__main__":
    logging.info("Pipeline start ....")
    main()