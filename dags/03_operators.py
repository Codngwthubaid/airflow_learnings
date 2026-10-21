from airflow.sdk import dag, task

@dag
def operators():
    @task.python
    def py_task():
        print("this is the python task")

    @task.bash
    def sh_task():
        return "echo https://codngwthubaid.vercel.app"

    python_task = py_task()
    bash_task = sh_task()

    python_task >> bash_task

operators()