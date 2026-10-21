from airflow.sdk import asset, task
from assets_13 import fetch_data
import os

@asset(
    # name of the asset
    name="process_data",
    # set the scheduling of the asset - this asset is only run after the fetch_data asset will run because this was dependent on that 
    schedule=fetch_data,
    # set the path at where we are storing out file im docker
    uri="/opt/airflow/logs/data/data_process.txt"
)

def process_data(self):
    # setting the dir 
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # writing the data into the folder data
    with open(self.uri, "w") as file:
        file.write("Data processed on successfully")   

    print(f"Data processed at : {self.uri}")