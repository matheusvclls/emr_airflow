# Import libraries
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)


# Variables
BUCKET_NAME = '<nome-do-seu-bucket>' 
LOCAL_DATA = "./data/profile_user.json"
S3_DATA = "data/landing_zone/profile_user.json"
LOCAL_SCRIPT = "./pyspark_script/modeling_profile_user_data.py"
S3_SCRIPT = "scripts/modeling_profile_user_data.py"


# Spark Configurations
JOB_FLOW_OVERRIDES = {
    "Name": "Modeling profile user data",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


# Steps to run
SPARK_STEPS = [
    {
        "Name": "Modeling data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar",
                            "Args": ["spark-submit",
                                    "--deploy-mode",
                                    "client",
                                    "s3://{{ params.BUCKET_NAME }}/{{ params.S3_SCRIPT }}",
                                    ],
                        },
    },
]


# Function to send files from local to s3
def from_local_to_s3(local_filename, s3_key, s3_bucket_name=BUCKET_NAME):
    '''
    Function responsible to send files, as scripts and .json/csv, from local to s3 bucket
    
    Parameters:
        local_filename (str): path in Airflow
        s3_key (str): key in s3 (without bucket name)
        s3_bucket_name (str): bucket name
    
    Return:
        None    
    '''

    s3 = S3Hook()
    s3.load_file(filename=local_filename, bucket_name=s3_bucket_name, replace=True, key=s3_key)


# Set default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 3, 5),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG
with DAG(
    "emr_and_airflow_integration",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=False
) as dag:

    # Only display - start_dag
    start_dag = DummyOperator(task_id="start_dag")

    # Send data to s3
    send_data_to_s3 = PythonOperator(
        dag=dag,
        task_id="send_data_to_s3",
        python_callable=from_local_to_s3,
        op_kwargs={"local_filename": LOCAL_DATA, "s3_key": S3_DATA,},
    )

    # Send script to s3
    send_script_to_s3 = PythonOperator(
        dag=dag,
        task_id="send_script_to_s3",
        python_callable=from_local_to_s3,
        op_kwargs={"local_filename": LOCAL_SCRIPT, "s3_key": S3_SCRIPT,},
    )

    # Create the EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )

    # Add the steps to the EMR cluster
    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "S3_SCRIPT": S3_SCRIPT,
        },
    )
    last_step = len(SPARK_STEPS) - 1

    # Wait executions of all steps
    check_execution_steps = EmrStepSensor(
        task_id="check_execution_steps",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="aws_default",
    )

    # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    # Only display - end_dag
    end_dag = DummyOperator(task_id="end_dag")

    # Data pipeline flow
    start_dag >> [send_data_to_s3, send_script_to_s3, create_emr_cluster] >> add_steps
    add_steps >> check_execution_steps >> terminate_emr_cluster >> end_dag
