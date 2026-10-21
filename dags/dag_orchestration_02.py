from airflow.sdk import dag, task

@dag
def dag_org_02():

    @task.python
    def dagorg_task1():
        print("this is first task of second dag org ...")

    @task.python
    def dagorg_task2():
        print("this is second task of second dag org ...")

    @task.python
    def dagorg_task3():
        print("this is third task of second dag org ...")

    task_1 = dagorg_task1()
    task_2 = dagorg_task2()
    task_3 = dagorg_task3()

    task_1 >> task_2 >> task_3

dag_org_02()