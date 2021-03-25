import datetime as datetime 
import os 
import json

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



def create_datset(client, dataset_name):

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project, dataset_name)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    

def get_bigquery_schemafields_from_json(path_to_json):
    with open(path_to_json) as f:
        dtypes = json.load(f)

    names = [field.get('column_name') for field in dtypes]
    field_types = [field.get('data_type') for field in dtypes]
    modes = [field.get('is_nullable') for field in dtypes]
    schema = [
        bigquery.SchemaField(
            name = name, 
            field_type = field_type,
            mode = "NULLABLE" if mode == 'YES' else "REQUIRED"
            )
        for name, field_type, mode in zip(names, field_types, modes)
    ]
    return schema 