from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

import requests
import json
import time

from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning) # disable warnings


# Replace with your own ID!
processor_id = '44bf077b-0180-1000-ffff-fffffaca68c0'  


def get_nifi_processor():
    print("Testing connection to NiFi...")
    CONN_ID = 'mynifi_connection'
    CONN_NIFI = BaseHook.get_connection(CONN_ID)
    url_nifi_api =  CONN_NIFI.host + ":" + str(CONN_NIFI.port) + "/nifi-api"

    header =  {'Content-Type':'application/json'} 
    rest_endpoint = f'{url_nifi_api}/processors/{processor_id}'
    print(f"Rest endpoint1: {rest_endpoint}")
    response = requests.get(rest_endpoint
                            , headers=header
                            , verify=False)
    print(f"Response: {response}")
    print(f"Processor JSON: {json.loads(response.content)}")
    print("Finished connection-test to NiFi.")

def startup():
    CONN_ID = 'mynifi_connection'
    CONN_NIFI = BaseHook.get_connection(CONN_ID)
    url_nifi_api = CONN_NIFI.host + ":" + str(CONN_NIFI.port) + "/nifi-api"

    response = update_processor_status(processor_id, "RUNNING", url_nifi_api)
    print(response)
     # wait for 120 seconds to give NiFi time to create a flow file
    time.sleep(120)
    response = update_processor_status(processor_id, "STOPPED", url_nifi_api)
    print(response)

def update_processor_status(processor_id: str, new_state: str, url_nifi_api):
    """Starts or stops a processor by retrieving the processor to get
    the current revision and finally putting a JSON with the desired
    state towards the API.

    :param processor_id: Id of the processor to receive the new state.
    :param new_state: String representing the new state, acceptable
                      values are: STOPPED or RUNNING.
    :param token: a JWT access token for NiFi.
    :param url_nifi_api: URL to the NiFi API
    :return: None
    """
    
    header =  {'Content-Type':'application/json'} 
    rest_endpoint = f'{url_nifi_api}/processors/{processor_id}'
    print(f"Rest endpoint2: {rest_endpoint}")
     # GET processor and parse to JSON
    response = requests.get(rest_endpoint
                            , headers=header
                            , verify=False)
    
    processor = json.loads(response.content)

    # Create a JSON with the new state and the processor's revision
    put_dict = {
        "revision": processor["revision"],
        "state": new_state,
        "disconnectedNodeAcknowledged": True,
    }

    # Dump JSON and POST processor
    payload = json.dumps(put_dict).encode("utf8")
    header = {
        "Content-Type": "application/json"
    }
    rest_endpoint = f'{url_nifi_api}/processors/{processor_id}/run-status'

    print(f"Rest endpoint3: {rest_endpoint}")
    response = requests.put( rest_endpoint,
        headers=header,
        data=payload,
    )
    return response

def get_token(url_nifi_api: str, access_payload: dict):
    header = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
    }
    response = requests.post(
        url_nifi_api + "access/token", headers=header
    )
    print(response)
    return response.content.decode("ascii")


def finalize():
    pass


with DAG(
    dag_id='hello_nifi',
    start_date=datetime(2021, 9, 5),  
    schedule_interval=None,
    catchup=False,
    tags=['something'],
    ) as dag:

    getprocessor_task = PythonOperator(
        task_id='get_nifi_processor_task',
        python_callable=get_nifi_processor,
        provide_context=False,
    )

    startup_task = PythonOperator(
        task_id="startup_task",
        python_callable=startup,
    )

    finalization = PythonOperator(
        task_id="finalization",
        python_callable=finalize,
    )

    getprocessor_task > startup_task > finalization