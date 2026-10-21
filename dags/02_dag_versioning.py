from airflow.sdk import dag, task

@dag
def dag_version():
    @task.python
    def DV_first():
        print("this is the first DV_first")

    @task.python
    def DV_second():
        print("this is the second DV_second")

    first = DV_first()
    second = DV_second()

    first >> second

dag_version()