from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime

@dag(
    dag_id="incremental_load",
    schedule=CronTriggerTimetable("@daily", timezone="Asia/Kolkata"),
    start_date=datetime(year=2026, month=10, day=15, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=10, day=31, tz="Asia/Kolkata"),
    catchup=True
)
def incremental_load():
    
    @task.python
    def fetching_data(**kwargs):
        print("In this task we FETCH data on the basis of time interval")
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")

    @task.bash
    def processing_data():
        return "echo 'Fetching data from {{data_interval_start}} to {{data_interval_end}}'"

    fetch_data = fetching_data()
    process_data = processing_data()

    fetch_data >> process_data

incremental_load()