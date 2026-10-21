from airflow.sdk import asset, task
from pendulum import datetime
import os

@asset(
    # name of the asset
    name="fetch_data",
    # set the scheduling of the asset
    schedule="@daily",
    # set the path at where we are storing out file im docker
    uri="/opt/airflow/logs/data/data.txt"
)

def fetch_data(self):
    # setting the dir 
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # writing the data into the folder data
    with open(self.uri, "w") as file:
        file.write("Data fetched on successfully")   

    print(f"Data written at : {self.uri}")