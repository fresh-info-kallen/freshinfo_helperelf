import datetime as datetime 
import os 
import json
import pprint 
import sys 

import pandas as pd 
from psycopg2.extras import Json
from google.cloud import bigquery

from freshinfo_helperelf import dbutils
from freshinfo_helperelf.gbq_dbutils import upload_df_to_gbq_table

def generate_system_error_message(error = None):
    if error is not None:
        if sys.exc_info()[0] is not None:
            system_error_message = "The system error message was: {} - {}".format(sys.exc_info()[0].__name__, str(error))
        else:
            system_error_message = "The system error message was: {}".format(str(error))
    else:
        system_error_message = "No system error message is available"
    return system_error_message

class add_job_task_record_error(Exception):
    def __init__(self, error):
        self.error = error 
        self.result_message = "The job task could not be added. {}.".format(generate_system_error_message(error))

class update_job_task_record_error(Exception):
    def __init__(self, error):
        self.error = error 
        self.result_message = "The job task could not be updated. {}.".format(generate_system_error_message(error))

def add_job_task_record(
    conn,
    job_id, 
    job_task_id, 
    job_task_description, 
    ts_start, 
    source_reference = None, 
    json_reference = None):

    try:
        job_task_results = {
            # Fields Populated when creating task 
            'job_id': job_id, 
            'job_task_id': job_task_id,
            'job_task_description': job_task_description,
            'ts_start': ts_start,
            'etl_task_status_id': dbutils.Etl.TaskStatus.start.value,

            # References
            'source_reference': source_reference,
            'json_reference': json_reference
        }

        # Convert to dataframe and upload to table
        job_task_results_data_frame = pd.DataFrame(pd.Series(job_task_results)).transpose()
        results = dbutils.IO.upload_df_to_table(
            conn = conn,
            df = job_task_results_data_frame,
            schema = 'etl',
            table = 'job_task_record'
        )

        if results.result_code != 0:
            raise add_job_task_record_error(error = results.error)
    
    except add_job_task_record_error as e:
        with open("{}_{}_add_job_task_record_error.txt".format(job_id, job_task_id), 'w') as file:
            file.write(e.result_message) 


    return job_task_results

def update_job_task_record(
    conn,
    job_id, 
    job_task_id, 
    ts_end, 
    result_code, 
    result_message, 
    affected_rows, 
    etl_task_status_id, 
    json_reference = None):

    this_result = dbutils.IO.ResultStats

    json_reference = json.dumps(json_reference)
    sql_update_statement = f"""
        UPDATE etl.job_task_record 
        SET 
            ts_end = '{ts_end}',
            result_code = {result_code},
            result_message = '{result_message}',
            affected_rows = {affected_rows},
            etl_task_status_id = {etl_task_status_id},
            json_reference = {Json(json_reference)}

        WHERE 
            job_id = {job_id} AND job_task_id = {job_task_id}
    """

    with conn.cursor() as cur:
        try:
            cur.execute(sql_update_statement)
            conn.commit()
            this_result.result_code = 0
            print("job task successfully updated")
        except Exception as e:
            conn.rollback()
            this_result.result_code = 1
            raise update_job_task_record_error(error = e)



def wrapped_job_task(
    function, 
    _job_id, 
    _job_task_id, 
    _job_task_description, 
    _source_reference,
    _conn, 
    **kwargs):
    # Create Task
    ts_start = str(datetime.datetime.now())
    try:
        task_start = add_job_task_record(
            conn = _conn,
            job_id = _job_id, 
            job_task_id = _job_task_id, 
            job_task_description = _job_task_description,  
            source_reference = _source_reference,
            ts_start = str(datetime.datetime.now()))
    except add_job_task_record_error as e:
        print(e.result_message)
        return e

    # Perform Task
    result = function(**kwargs)

    # Get Results
    etl_job_task_fields = ['result_code', 'result_message', 'affected_rows', 'etl_task_status_id', 'json_reference']
    etl_task_results = {key: value for key, value in result.__dict__.items() if key in etl_job_task_fields}

    # End the clock and update job task record with results
    ts_end = str(datetime.datetime.now())

    try:
        update_job_task_record(
            conn = _conn,
            job_id = _job_id, 
            job_task_id = _job_task_id, 
            ts_end = ts_end, 
            **etl_task_results)
    except update_job_task_record_error as e:
        print(e.result_message)
        raise Exception("Failed to update job task")
        return e
        
    pprint.pprint(etl_task_results)
    return result 

def fetch_next_job_id(conn):
    
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT job_id FROM etl.job_task_record ORDER BY job_id DESC LIMIT 1")
            job_id = cur.fetchone()
            return job_id[0] + 1
        except Exception as e:
            print(e)
            conn.rollback()