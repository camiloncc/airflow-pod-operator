#HERE DECLARE LIBS AND OPERATORS
import airflow
from airflow import DAG
from airflow.operators.sensors import TimeDeltaSensor
from airflow.models import Variable
from datetime import datetime, timedelta, date, time
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.pod import Resources
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow import configuration as conf
from airflow.utils.email import send_email
import logging
import os
import sys
import glob


#HERE DECLARE DAG DEFINITION
start = datetime.today() - timedelta(days=1)
default_args = {
    'owner': 'Udacity-DAG',
    'start_date': start,
    'email': 'carrascocamilo@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG('00_DAG_UDACITY_POD_OPERATOR', default_args=default_args, schedule_interval="0 0 * * 1-5",tags=['GIT','POD OPERATOR'])


#HERE DECLARE VOLUME AND MOUNT FROM AZURE PVC 
volume_mounts  = [VolumeMount('airflow-files', mount_path='/data',sub_path=None, read_only=False)]
volumes  = [Volume(name='airflow-files', configs={'persistentVolumeClaim': {'claimName': 'azfile-airflow-tmpfiles'}})]

#HERE DECLARE POD OPERATOR
PodOperatorExample = KubernetesPodOperator(
    namespace='udacity-airflow',
    image='name-cluster.azurecr.io/name-image-docker-registry:tag-image', #define Image for pod, source dockerHub or AzureRegistry
    task_id='00_PodOperatorUdacity',
    xcom_push=True,
    xcom_all=True,
    executor_config={
        "KubernetesExecutor": {"request_memory": "5632Mi", "limit_memory": "5632Mi", "request_cpu": "3000m","limit_cpu": "3000m"} #define resouces from cluster
                    },
    pool='pool_udacity', #define pool task 
    name='declare_PodOperatorUdacity', #define name is requerired for generate pod name 
    priority_weight=10,
    wait_for_downstream=True,
    cmds=["bash", "-c"], #invoke bash inside container
    arguments=["python /opt/process_input.py"], #run my script with files param csv 
    env_vars={
        'INPUT_PATH': '/data/input_csv_before_process.csv',
        'OUTPUT_PATH': '/data/input_csv_after_process.csv'
    },
    volumes=volumes, #Use Volume
    volume_mounts=volume_mounts, #mount Volume
    is_delete_operator_pod=True, #delete pod after run
    dag=dag)



