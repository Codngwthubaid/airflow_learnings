from airflow.sdk import dag, task
from airflow.timetables.events import EventsTimetable
from pendulum import datetime

special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=4, day=17, tz='Asia/Kolkata'),
        datetime(year=2026, month=4, day=17, tz='Asia/Kolkata')
    ]
)

@dag(
    dag_id="custom_scheduling_dates",
    start_date=datetime(year=2026, month=1, day=1, tz='Asia/Kolkata'),
    end_date=datetime(year=2026, month=12, day=31, tz='Asia/Kolkata'),
    catchup=True,
    is_paused_upon_creation=False
)

def custom_scheduling_dates():
    @task.python
    
    def extraction(**kwargs):
        date = kwargs['logical_date']
        print(f"this is an extraction task on the basis of custom scheduling date - {date}")

        extraction_date = date
        
custom_scheduling_dates()

    