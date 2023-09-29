import os
import requests
import pandas as pd
import logging
import logging.config
from google.cloud import storage
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\Users\\WTA23811\\etl-python-400407-43615368880a.json"


def create_bucket(bucket_name, location):
    """Creates a new GCS bucket if it doesn't already exist."""
    storage_client = storage.Client()

    # Vérifier si le seau existe déjà
    bucket = storage_client.bucket(bucket_name)

    if not bucket.exists():
        # Si le seau n'existe pas, le créer
        bucket = storage_client.create_bucket(bucket_name, location=location)
        print(f"Bucket {bucket.name} created in {bucket.location}")
    else:
        print(f"Bucket {bucket.name} already exists.")

# Appel de la fonction pour créer un seau (bucket) avec vérification d'existence
create_bucket("my-data-8", "us-central1")