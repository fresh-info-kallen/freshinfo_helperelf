import datetime as datetime 
import os 

from google.cloud import bigquery
from freshinfo_helperelf import dbutils


def get_gbq_client(gcp_project, path_to_key):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_key 
    client = bigquery.Client(project = gcp_project)
    return client 

def upload_df_to_gbq_table(
    data_frame, 
    client, 
    dataset_id, 
    table_id, 
    schema = None, 
    if_exists = 'append'):

    # Start the clock
    this_result = dbutils.IO.ResultStats()
    this_result.ts_start = datetime.datetime.now()

    try:
        # Add schema 
        job_config = bigquery.LoadJobConfig(schema = schema)

        if if_exists == 'replace':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif if_exists == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif if_exists == 'fail':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
            
        # Create Job to load table from dataframe 
        job = client.load_table_from_dataframe(
            data_frame, 
            destination = "{}.{}.{}".format(client.project, dataset_id, table_id), 
            job_config = job_config
        )

        # Get json result
        json_reference = job.result()._properties

        res_msg = "Dataframe uploaded to big query."
        res_code = 0
        affected_rows = len(data_frame)
        task_status_id = dbutils.Etl.TaskStatus.success.value

    except Exception as error:
        res_msg = ("Error: %s" % error)
        res_code = 1
        affected_rows = 0
        task_status_id = dbutils.Etl.TaskStatus.failure.value 
        this_result.error = Exception(error)
        json_reference = None
    finally:
        this_result.ts_end = datetime.datetime.now()
        this_result.duration_timedelta = this_result.ts_end - this_result.ts_start
        this_result.duration_message = dbutils.IO._IO__duration_in_minutes(this_result.duration_timedelta.seconds)
        this_result.result_message = res_msg
        this_result.result_code = res_code
        this_result.affected_rows = affected_rows
        this_result.etl_task_status_id = task_status_id

        this_result.json_reference = json_reference
        
    # Return Job Results 
    return this_result 

    # Start the clock
    this_result = dbutils.IO.ResultStats()
    this_result.ts_start = datetime.datetime.now()

    try:
        # Add schema 
        job_config = bigquery.LoadJobConfig(schema = schema)

        # Create Job to load table from dataframe 
        job = client.load_table_from_dataframe(
            data_frame, 
            destination = "{}.{}.{}".format(table_reference.project, table_reference.dataset_id, table_reference.table_id), 
            job_config = job_config
        )

        # Get json result
        json_reference = job.result()._properties

        res_msg = "Dataframe uploaded to big query."
        res_code = 0
        affected_rows = len(data_frame)
        task_status_id = dbutils.Etl.TaskStatus.success.value

    except Exception as error:
        res_msg = ("Error: %s" % error)
        res_code = 1
        affected_rows = 0
        task_status_id = dbutils.Etl.TaskStatus.failure.value 
        this_result.error = Exception(error)
        json_reference = None
    finally:
        this_result.ts_end = datetime.datetime.now()
        this_result.duration_timedelta = this_result.ts_end - this_result.ts_start
        this_result.duration_message = dbutils.IO._IO__duration_in_minutes(this_result.duration_timedelta.seconds)
        this_result.result_message = res_msg
        this_result.result_code = res_code
        this_result.affected_rows = affected_rows
        this_result.etl_task_status_id = task_status_id

        this_result.json_reference = json_reference
        
    # Return Job Results 
    return this_result 