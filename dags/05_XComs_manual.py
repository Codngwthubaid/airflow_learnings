from airflow.sdk import dag, task

@dag
def XComs_manual():
    @task.python 
    def ft(**kwargs):
        
        # extracting 'ti' from kwargs 
        ti = kwargs['ti']
        print("this is the first task of manual_xcoms")
        data = {"data" : ["ubaid", "manjot", "manjeet", "shaurya", "parul", "piyush"]}
        ti.xcom_push(key="result", value=data)

    @task.python
    def st(**kwargs):
        # extracting 'ti' from kwargs 
        ti = kwargs['ti']

        # pulling data through 'ti'
        data = ti.xcom_pull(key='result', task_ids='ft')
        print("this is the second task of manual_xcoms")
        transformed_data = data['data']
        transformed_dict_data = {'trans_data' : transformed_data}

        # pushing data though 'ti'
        ti.xcom_push(key='result', value=transformed_dict_data)

    @task.python
    def tt(**kwargs):
        # extracting 'ti' from kwargs 
        ti = kwargs['ti']

        load_data = ti.xcom_pull(key='result', task_ids='st')
        return load_data 

    first_task = ft()
    second_task = st()
    third_task = tt()

    first_task >> second_task >> third_task

XComs_manual()