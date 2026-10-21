from airflow.sdk import dag, task
from dag_orchestration_01 import dag_org_01
from dag_orchestration_02 import dag_org_02
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag
def dag_parent():

    # By doing this approach we run the parent dag but actually dag_org_01 and dag_org_02 are not actually runs

    # @task
    # def task_1():
    #     dag_org_01()

    # @task
    # def task_2():
    #     dag_org_02()

    # task_1() >> task2()

    # by achiving this in the parent DAG we want to import the function i.e TriggerDagRunOperator because to actually run a DAG we need an operator - we can't only call the func from another file and run the DAG

    trigger_first_dag = TriggerDagRunOperator(
        task_id = "trigger_first_dag_org",
        trigger_dag_id="dag_org_01",

    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id = "trigger_second_dag_org",
        trigger_dag_id="dag_org_02",
        wait_for_completion=True                
    )

    trigger_first_dag >> trigger_second_dag

dag_parent()
