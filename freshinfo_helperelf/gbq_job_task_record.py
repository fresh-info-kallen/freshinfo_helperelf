import datetime as datetime 
import os 
import json
import pprint 

import pandas as pd 
from google.cloud import bigquery

from freshinfo_helperelf import dbutils
from freshinfo_helperelf.gbq_dbutils import upload_df_to_gbq_table

def add_job_task_record(
    job_id, 
    job_task_id, 
    job_task_description, 
    ts_start, 
    client,
    source_reference = None, 
    json_reference = None):

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

    SCHEMA = [
        # Fields Populated when creating task 
        bigquery.SchemaField("job_id", "INT64", mode = "REQUIRED"),
        bigquery.SchemaField("job_task_id", "INT64", mode = "REQUIRED"),
        bigquery.SchemaField("job_task_description", "STRING", mode = "REQUIRED"),
        bigquery.SchemaField("ts_start", "STRING", mode = "NULLABLE"),
        bigquery.SchemaField("etl_task_status_id", "INT64", mode = "NULLABLE"),

        # References
        bigquery.SchemaField("source_reference", "STRING", mode = "NULLABLE"),
        bigquery.SchemaField("json_reference", "STRING", mode = "NULLABLE")
    ]

    # Convert to dataframe and upload to table
    job_task_results_data_frame = pd.DataFrame(pd.Series(job_task_results)).transpose()
    results = upload_df_to_gbq_table(
        job_task_results_data_frame, 
        client, 
        dataset_id = 'etl',
        table_id = 'job_task_record',
        schema = SCHEMA)

    # pprint.pprint(results.__dict__)

    return job_task_results

def update_job_task_record(
    job_id, 
    job_task_id, 
    ts_end, 
    result_code, 
    result_message, 
    affected_rows, 
    etl_task_status_id, 
    client,
    json_reference = None):

    job_task_results = {
        # ID
        'job_id': job_id, 
        'job_task_id': job_task_id,

        # Update Results
        'ts_end': str(ts_end),
        'result_code': result_code,
        'result_message': result_message,
        'affected_rows': affected_rows,
        'etl_task_status_id': dbutils.Etl.TaskStatus.start.value,

        # References
        'json_reference': json.dumps(json_reference)
    }

    job_task_results_data_frame = pd.DataFrame(pd.Series(job_task_results)).transpose()
    SCHEMA = [
            # Job and Job Task Identifier 
            bigquery.SchemaField("job_id", "INT64", mode = "REQUIRED"),
            bigquery.SchemaField("job_task_id", "INT64", mode = "REQUIRED"),

            # # Update Fields
            bigquery.SchemaField("ts_end", "STRING", mode = "NULLABLE"),
            bigquery.SchemaField("result_code", "INT64", mode = "NULLABLE"),
            bigquery.SchemaField("result_message", "STRING", mode = "NULLABLE"),
            bigquery.SchemaField("affected_rows", "INT64", mode = "NULLABLE"),
            bigquery.SchemaField("etl_task_status_id", "INT64", mode = "NULLABLE"),

            # # References
            bigquery.SchemaField("json_reference", "STRING", mode = "NULLABLE")
        ]

    results = upload_df_to_gbq_table(
        job_task_results_data_frame, 
        client, 
        dataset_id = 'etl',
        table_id = 'job_task_record_updates',
        schema = SCHEMA)

    if results.result_code != 0 :
        print(3)
        pprint.pprint(results.__dict__)
        
        raise Exception("An error occurred while uploading the updated job task record to gbq")

    

    # Perform a query to update the corresponding job/job_task row in the job_task_record table
    QUERY = ("""
        UPDATE `ubermediadata.etl.job_task_record` AS t
        SET 
            t.ts_end = s.ts_end,
            -- t.duration_message = s.duration_message,
            t.result_code = s.result_code,
            t.result_message = s.result_message,
            t.affected_rows = s.affected_rows,
            t.etl_task_status_id = s.etl_task_status_id,
            t.json_reference = s.json_reference
            
        FROM `ubermediadata.etl.job_task_record_updates` AS s
        WHERE t.job_id = s.job_id 
        AND t.job_task_id = s.job_task_id
        AND s.job_id = {} AND s.job_task_id = {}
    """.format(job_id, job_task_id))

    job = client.query(QUERY)
    result = job.result() # Raises an exception if there is something wrong with the query

    # return job_task_results
    return results 



def wrapped_job_task(
    function, 
    _job_id, 
    _job_task_id, 
    _job_task_description, 
    _source_reference,
    _client, 
    **kwargs):
    # Create Task
    ts_start = str(datetime.datetime.now())
    task_start = add_job_task_record(
        job_id = _job_id, 
        job_task_id = _job_task_id, 
        job_task_description = _job_task_description,  
        source_reference = _source_reference,
        client = _client,
        ts_start = str(datetime.datetime.now()))

    # Perform Task
    result = function(**kwargs)

    # Get Results
    etl_job_task_fields = ['result_code', 'result_message', 'affected_rows', 'etl_task_status_id', 'json_reference']
    etl_task_results = {key: value for key, value in result.__dict__.items() if key in etl_job_task_fields}

    # End the clock and update job task record with results
    ts_end = str(datetime.datetime.now())
    task_end = update_job_task_record(
        job_id = _job_id, 
        job_task_id = _job_task_id, 
        ts_end = ts_end, 
        client = _client, 
        **etl_task_results)

    pprint.pprint(etl_task_results)
    return result 

def fetch_next_job_id(machine_id, client):
    query_string = """
    SELECT 
        job_id 
    FROM ubermediadata.etl.job_task_record 
    WHERE job_id > {} 
    AND job_id < {} 
    ORDER BY job_id DESC 
    LIMIT 1 
    """.format(machine_id, machine_id + 10000)

    job = client.query(query_string)
    df = job.result().to_dataframe()

    if len(df) > 0:
        return df.iloc[0].job_id + 1
    else:
        return machine_id + 1