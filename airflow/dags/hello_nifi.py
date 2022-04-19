from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule

import json
from datetime import datetime
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning) # disable warnings

def get_nifi_processor():
    print("Testing connection to NiFi...")
    CONN_ID = 'mynifi_connection'
    CONN_NIFI = BaseHook.get_connection(CONN_ID)
    nifi_api_url =  CONN_NIFI.host + ":" + str(CONN_NIFI.port) + "/nifi-api"

    # Replace with your own ID!
    processor_id = '288c8844-0180-1000-0000-0000282adb60'  
    # you could pass a bearer auth-token via the header!
    header =  {'Content-Type':'application/json'} 
    rest_endpoint = f'{nifi_api_url}/processors/{processor_id}'
    print(f"Rest endpoint: {rest_endpoint}")
     # GET processor and parse to JSON
    response = requests.get(rest_endpoint
                            , headers=header
                            , verify=False)
    print(f"Response: {response}")
    print(f"Processor JSON: {json.loads(response.content)}")
    print("Finished connection-test to NiFi.")


def on_failure():
    """The method gets executed only if at least one previous task fails."""
    print("something failed... cleaning up.")


with DAG(
    dag_id='hello_nifi',
    start_date=datetime(2021, 9, 5),  
    schedule_interval=None,
    catchup=False,
    tags=['something'],
    ) as dag:

    startup_task = PythonOperator(
        task_id='get_nifi_processor_task',
        python_callable=get_nifi_processor,
        provide_context=False,
    )

    failure_logging = PythonOperator(
        task_id='failure_logging',
        python_callable=on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=False,
    )

    startup_task > failure_logging