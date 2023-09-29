import os
import requests
import pandas as pd
import logging
import logging.config
from google.cloud import storage
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\Users\\WTA23811\\etl-python-400407-43615368880a.json"


log_file_abs = os.path.abspath('C:\\Users\\WTA23811\\etl-gcp\\src\\util\\logging_to_file.conf')
#log_file_abs = os.path.abspath("C:/Users/WTA23811/etl-gcp/src/util/logging_to_file.conf")
logging.config.fileConfig(fname=log_file_abs)
logger = logging.getLogger(__name__)

headers={
    "Accept": "application/json",
    "Content-Type": "application/json"
}

#file_name = "file_csv.csv"
def ingestion(url_input):
    try:
        file_name = "file_csv.csv"
        logger.info("load data from internet begin ...")
        response = requests.request("GET", url=url_input, headers=headers, data={})
        file_json = response.json()

        logger.info("Transformation to csv ...")
        file_dataframe = pd.DataFrame(file_json["data"])
        file_csv = file_dataframe.to_csv(file_name, index=False)

    except Exception as e:
        logger.error("Error ingestion - please check " + str(e), exc_info=True)

    return file_csv


def create_bucket(bucket_name, location):
    """Creates a new GCS bucket if it doesn't already exist."""
    try:
        logger.info(" create_bucket ...")
        storage_client = storage.Client()

        # Vérifier si le seau existe déjà
        bucket = storage_client.bucket(bucket_name)

        if not bucket.exists():
            # Si le seau n'existe pas, le créer
            bucket = storage_client.create_bucket(bucket_name, location=location)
            logger.info(f"Bucket {bucket.name} created in {bucket.location}")
        else:
            logger.info(f"Bucket {bucket.name} already exists.")

        # Attendre 20 minutes (120 secondes) avant de supprimer bucket
        #time.sleep(30)
        # Supprimer bucket
        #bucket.delete()
        #logger.info(f"Bucket {bucket.name} deleted.")

    except Exception as e:
        logger.error("Error create_bucket - please check " + str(e), exc_info=True)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
     #Uploads a file to a GCS bucket.
    try:
        logger.info(" upload_blob ...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
    except Exception as e:
        logger.error("Error upload_blob - please check " + str(e), exc_info=True)

"""""
def delete_locale_file(file_path):
    try:
        logger.info(" delete_locale_file ...")
        os.remove(file_path)
        logger.info(f"File '{file_path}' has been deleted successfully.")
    except FileNotFoundError:
        logger.error(f"File '{file_path}' not found.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
"""""
