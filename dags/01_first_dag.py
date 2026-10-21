# import dags and task
from airflow.sdk import dag, task

@dag # decorator - modern way to declare a DAG
def first_dag(): # function for the DAG
    @task.python  
    def first_task():
        print("this is my first task!")

    @task.python
    def second_task():
        print("this is my second task!")

    @task.python
    def third_task():
        print("this is my second task!")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

first_dag()
