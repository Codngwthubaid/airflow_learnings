from airflow.sdk import dag, task
from pendulum import datetime

# defining schedule parameters
@dag(
    # name of the DAG
    dag_id="preset_schedule",
    # start time of DAG define by using pendulum
    start_date= datetime(year=2026, month=1, day=1, tz="Asia/Kolkata"),
    # daily 12:00 this DAG will schedule
    schedule="@daily",
    # DAG should be paused when it is registered in the database for the very first time.
    is_paused_upon_creation=False,
    # used to tell airflow to run the DAG for the missing intervals, by default it is false
    catchup=True
)

def preset_schedule():

    @task.python
    def fst():
        print("this is first schedule task")

    @task.python
    def sst():
        print("this is second schedule task")

    @task.python
    def tst():
        print("this is third schedule task")

    first_schedule_task = fst()
    second_schedule_task = sst()
    third_schedule_task = tst()

    first_schedule_task >> second_schedule_task >> third_schedule_task

preset_schedule()