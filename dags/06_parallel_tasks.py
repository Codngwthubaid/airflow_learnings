from airflow.sdk import dag, task

@dag
def parallel_task():
    
    @task.python
    def extraction(**kwargs):

        print("Extracting data ...")
        ti = kwargs["ti"]
        data = {
            "api_data" : [1,2,3,4,5],
            "db_data" : [6,7,8,9,10],
            "s3_data" : [11,12,13,14,15]
        }
        ti.xcom_push(key="result", value=data)

    @task.python
    def transform_api_data(**kwargs):
        print("Transforming API data ...")
        ti = kwargs["ti"]
        extracted_api_data = ti.xcom_pull(task_ids="extraction", key="result")["api_data"]
        print("Extracting API data : ", extracted_api_data)
        transformation = [i*10 for i in extracted_api_data]
        ti.xcom_push(key="result", value=transformation)

    @task.python
    def transform_db_data(**kwargs):
        print("Transforming DB data ...")
        ti = kwargs["ti"]
        extracted_db_data = ti.xcom_pull(task_ids="extraction", key="result")["db_data"]
        print("Extracting API data : ", extracted_db_data)
        transformation = [i*10 for i in extracted_db_data]
        ti.xcom_push(key="result", value=transformation)

    @task.python
    def transform_s3_data(**kwargs):
        print("Transforming S3 data ...")
        ti = kwargs["ti"]
        extracted_s3_data = ti.xcom_pull(task_ids="extraction", key="result")["s3_data"]
        print("Extracting API data : ", extracted_s3_data)
        transformation = [i*10 for i in extracted_s3_data]
        ti.xcom_push(key="result", value=transformation)

    @task.bash
    def load_data(**kwargs):
        print("Loading data ...")
        ti = kwargs["ti"]
        api_data = ti.xcom_pull(task_ids="transform_api_data", key="result")
        db_data = ti.xcom_pull(task_ids="transform_db_data", key="result")
        s3_data = ti.xcom_pull(task_ids="transform_s3_data", key="result")

        return f"echo 'loaded data : {api_data} \n, {db_data} \n, {s3_data}'"

    extraction_data_task = extraction()
    trans_api_data_task = transform_api_data()
    trans_db_data_task = transform_db_data()
    trans_s3_data_task = transform_s3_data()
    load_data_task = load_data()

    extraction_data_task >> [trans_api_data_task, trans_db_data_task, trans_s3_data_task] >> load_data_task

parallel_task()