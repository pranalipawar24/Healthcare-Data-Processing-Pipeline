from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='simple_emr_dag',
    default_args=default_args,
    schedule_interval='0 12 * * *',
    catchup=False,
)

# Define the EMR steps for each Spark job
SPARK_STEP_1 = [
    {
        'Name': 'Run Spark Job 1',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'yarn',
                '--driver-memory', '4G',
                '--executor-memory', '2G',
                '--num-executors', '4',
                '--executor-cores', '2',
                '--jars', 's3://datawarehouse360/jar/postgresql-42.2.14.jar',
                's3://datawarehouse360/python/rdbms_read_and_write_to_s3.py',
            ],
        },
    }
]

SPARK_STEP_2 = [
    {
        'Name': 'Run Spark Job 2',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'yarn',
                '--driver-memory', '4G',
                '--executor-memory', '2G',
                '--num-executors', '4',
                '--executor-cores', '2',
                '--jars', 's3://datawarehouse360/jar/postgresql-42.2.14.jar',
                's3://datawarehouse360/python/read_from_s3_and_transform_to_hive.py',
            ],
        },
    }
]

SPARK_STEP_3 = [
    {
        'Name': 'Run Spark Job 3',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            \
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'yarn',
                '--driver-memory', '4G',
                '--executor-memory', '2G',
                '--num-executors', '4',
                '--executor-cores', '2',
                '--jars', 's3://datawarehouse360/jar/postgresql-42.2.14.jar',
                's3://datawarehouse360/python/spark_hive_write_to_s3.py',
            ],
        },
    }
]

start = DummyOperator(task_id='start', dag=dag)

# Add steps to the existing EMR cluster
rdbmsread_to_s3 = EmrAddStepsOperator(
    task_id='rdbmsread_to_s3',
    job_flow_id='j-1338IE0V7ZI2G',  # Replace with your active EMR cluster ID
    steps=SPARK_STEP_1,
    aws_conn_id='aws_default',  # Ensure this connection is set up in Airflow
    dag=dag,
)

s3_transform_hive = EmrAddStepsOperator(
    task_id='s3_transform_hive',
    job_flow_id='j-1338IE0V7ZI2G',  # Replace with your active EMR cluster ID
    steps=SPARK_STEP_2,
    aws_conn_id='aws_default',
    dag=dag,
)

hive_to_s3 = EmrAddStepsOperator(
    task_id='hive_to_s3',
    job_flow_id='j-1338IE0V7ZI2G',  # Replace with your active EMR cluster ID
    steps=SPARK_STEP_3,
    aws_conn_id='aws_default',
    dag=dag,
)


end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> rdbmsread_to_s3 >> s3_transform_hive >> hive_to_s3 >> end

# If the tasks are independent, you can use this instead:
# [add_emr_steps_1, add_emr_steps_2, add_emr_steps_3]
