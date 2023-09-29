from setuptools import setup, find_packages

setup(
    name='etl-gcp',
    version='0.0.1',
    description='etl from internet to gcp (gcs, pub/sub, dataflow, dataproc and BigQuery',
    author='open',
    packages=find_packages(),
)