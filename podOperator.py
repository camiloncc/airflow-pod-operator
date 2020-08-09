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
