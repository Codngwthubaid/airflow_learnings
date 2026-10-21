from airflow.sdk import dag, task

@dag
def XCom_default():
    @task.python
    def first_task():
        print("this is the first task!")
        data = {"dict" : [1,2,3,4,5]}
        return data

    @task.python
    def second_task(data:dict):
        print("this is the second task!")
        transformed_data = data['dict']
        transformed_dict_data = {"trans_data" : transformed_data}
        return transformed_dict_data

    @task.python
    def third_task(data:dict):
        print("this is the third task!")
        load_data = data
        return load_data

    ft = first_task()
    st = second_task(ft)
    tt = third_task(st)


XCom_default()