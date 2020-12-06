import datetime as datetime
import numpy as np 
import pandas as pd 
import psycopg2 as pg 
import enum
from io import StringIO as StringIO

class Etl:
# these task type values correspond with the content of table 'etl.job_task_type'.
        class TaskType(enum.Enum):
                read    = 1
                write   = 2
                recon   = 3
                copy    = 4
                inform  = 5
                report  = 6

# these task type values correspond with the content of table 'etl.job_task_status'.
        class TaskStatus(enum.Enum):
                start     = 1
                success   = 2
                failure   = 3
                warning   = 4
                partial   = 5 

class IO:

        class ResultStats:
                ts_start : datetime.datetime
                ts_end : datetime.datetime
                duration_timedelta : datetime.timedelta
                duration_message : str
                result_message : str
                result_code : np.int
                affected_rows : np.int64
                etl_task_status_id : np.int
                error : Exception
                const_dateformat : str = r"%Y-%m-%d %H:%M:%S"
                data_frame: pd.DataFrame

                def __init__(self): # initialise new objects with some default values
                        self.ts_start = datetime.datetime.strptime('1900-01-01 00:00:00', self.const_dateformat)
                        self.ts_end = datetime.datetime.strptime('1900-01-01 00:00:00', self.const_dateformat)
                        self.duration_message = "No duration"
                        self.result_message = "No result"
                        self.result_code = 0
                        self.affected_rows = 0
                        self.etl_task_status_id = 0

                # shortcut method for easy printing of results
                def show_results(self):
                        return \
                        "start time: " + self.ts_start.strftime(self.const_dateformat) + "\n" +\
                        "end time: " + self.ts_end.strftime(self.const_dateformat) + "\n" +\
                        "duration: " + self.duration_message + "\n" +\
                        "affected rows: " + str(self.affected_rows) + "\n" +\
                        "result msg: " + self.result_message + "\n" +\
                        "result code: " + str(self.result_code)

        @staticmethod
        def __duration_in_minutes(seconds: np.int64):
                minutes = divmod(seconds, 60)
                return (str(minutes[0]) + 'minutes' + str(minutes[1]) + 'seconds')

        def upload_df_to_table(
                conn : pg._ext.connection,
                df : pd.DataFrame,
                schema: str,
                table : str,
                job_task_id : np.int64 = -1, # optionally specify an ETL job task ID if this is a scheduled job
                source_id_colname : str = None, # optionally name a column which is a source ref ID for each record
                null_str : str = '',  # override this if you want to specify a particular str val be uploaded as null to Postgres
                source_reference = None):

                # start the clock...
                this_result = IO.ResultStats()
                this_result.ts_start = datetime.datetime.now()

                # add standard ETL fields to the df using incoming args
                df_to_upload = IO.__prepare_df_for_etl(df, job_task_id, source_id_colname)
                cols_to_upload = tuple([df_to_upload.index.name]) + tuple(df_to_upload.columns)

                # save dataframe into in-memory buffer
                buffer = StringIO()
                df_to_upload.to_csv(buffer, sep="|", index_label='etl_dataframe_id', header=False, index=True, quotechar="~", line_terminator="\n")
                buffer.seek(0)
                cursor = conn.cursor()
                res_msg = "Copy not yet attempted..."
                try:
                        full_tablename = schema + "." + table
                        if null_str is not None:
                                cursor.copy_from(buffer,full_tablename,sep="|", columns=cols_to_upload, null=null_str) # copy to table straight from buffer
                        else:
                                cursor.copy_from(buffer,full_tablename,sep="|", columns=cols_to_upload)
                
                        conn.commit()
                        cursor.close()
                        res_msg = "Copy from stringio() to '" + full_tablename + "' completed."
                        res_code = 0
                        affected_rows = len(df.index)
                        task_status_id = Etl.TaskStatus.success.value
                except (Exception, pg.DatabaseError) as error:
                        res_msg = ("Error: %s" % error)
                        conn.rollback()
                        cursor.close()
                        res_code = 1
                        affected_rows = 0
                        task_status_id = Etl.TaskStatus.failure.value
                        this_result.error = Exception(error)
                        task_status_id = Etl.TaskStatus.failure.value
                        #raise Exception(error)
                finally:
                        # always populate the results object, regardless of success/failure
                        this_result.ts_end = datetime.datetime.now()
                        this_result.duration_timedelta = this_result.ts_end - this_result.ts_start
                        this_result.duration_message = IO.__duration_in_minutes(this_result.duration_timedelta.seconds)
                        this_result.result_message = res_msg
                        this_result.result_code = res_code
                        this_result.affected_rows = affected_rows
                        this_result.etl_task_status_id = task_status_id
                        this_result.source_reference = source_reference

                return this_result



        @staticmethod
        def __prepare_df_for_etl(df: pd.DataFrame, job_task_id, source_id_colname):
                # Standardise Columns
                df.columns = df.columns.str.lower().str.replace(' ', '_')

                # We want the index added to the table as if it were just another column
                df.index.name = 'etl_dataframe_id'

                return df