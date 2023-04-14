from awsglue.transforms import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from ingestion_framework.scripts import job_initializer
import sys
import boto3
from pytz import timezone
from datetime import datetime, timedelta,date
from ingestion_framework.utils.dynamodb_util import JobStatus
from ingestion_framework.utils import athena_util
from ingestion_framework.constants import projectConstants
from pyspark.sql.functions import md5, concat_ws,row_number,dense_rank,lit,col,max,when,date_sub,date_format,current_timestamp,coalesce
from pyspark.sql import Window,functions as F
from pyspark.sql.types import DateType
print("environment varialbe Initialization")
env = getResolvedOptions(sys.argv, ['JobEnvironment'])['JobEnvironment']  # "dev"
app_name = getResolvedOptions(sys.argv, ['AppName'])['AppName']  # "fee_rpt"
aws_region = getResolvedOptions(sys.argv, ['AWSRegion'])['AWSRegion']
domain_name = getResolvedOptions(sys.argv, ['DomainName'])['DomainName']  # "finance"
sts_client = boto3.client(projectConstants.STS)
aws_account_id = sts_client.get_caller_identity()[projectConstants.ACCOUNT]
dynamodb = boto3.resource('dynamodb', region_name=aws_region)
dynamo_table_name = 'datamesh-jobstatus-{}'.format(env)
table_name='fee_sat'
pipeline_id = 'fee_rpt_transform_job'
inv_dict = {'pipeline_id': pipeline_id, 
'target_type': 's3', 
'target_file_name': table_name , 
'load_frequency': 'daily', 
'load_type': 'full', 
'domain_name': domain_name, 
'load_phase': 'processed'}
# Initialize spark and logger
spark= job_initializer.initializeSpark(app_name)
logger = job_initializer.getGlueLogger(spark)
job_status = JobStatus(spark, logger)
job_start_time = datetime.now(timezone('US/Eastern'))
exec_id = ""
sla_met = ""
today_dt = date.today().strftime("%Y-%m-%d")
fee_rpt_prefix=f"data/processed/"
# S3 Buckets

fee_rpt_finance_s3_bucket=f"datamesh-finance-data-domain-{aws_account_id}-{env}-{aws_region}"
#Final Base Paths
finance_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/final/"
processed_base_s3path = f"s3a://{fee_rpt_finance_s3_bucket}/data/processed/"

#Processed S3 Path
processed_s3path = f"s3a://{fee_rpt_finance_s3_bucket}"+"/"+fee_rpt_prefix+table_name+"/"
index=0
tables_list = {
    "finance":["fee_info","long_fee_typ","fee_cd"]    
    }

hash_columns={
    "REGL_FEE_TYPE",
    "REGL_FEE_CLASS",
    "FEE_DESC",
    "EFFT_DT",
    "FEE_ID",
    "FEE_ID_TYPE"
}

fee_sat_sequence_columns=[
    "FEE_HUB_HASH",
    "REGL_FEE_TYPE",
    "REGL_FEE_CLASS",
    "FEE_DESC",
    "EFFT_DT",
    "DIFF_HASH",
    "SRC_SYS_CD",
    "LOAD_DTM",
    "FEE_ID",
    "FEE_ID_TYPE"
]   

with open('fee_cd.sql', "r") as f:
    globals()[f"fee_cd_query"] = f.read().format(env=env)
    print("read complete fee_cd_union") 

with open('fee_sat.sql', "r") as f:
    globals()[f"fee_sat_query"] = f.read().format(env=env)
    print("read complete fee_cd_sat")    

print("end of environment varialbe Initialization")

def spark_reader(base_s3path,table):
    return spark.read.format("parquet").load(f"{base_s3path}{table}/")
def temp_view_creator(table_df,table):
    table_df.createOrReplaceTempView(table)

def final_table_reader(base_s3path,tables_list):
    for table in tables_list:
        print(f'base s3 path is: {base_s3path}')
        globals()[f"df_{table}"]=spark_reader(base_s3path,table)
        temp_view_creator(globals()[f"df_{table}"],table)

def write_data_to_processed(f_df, target_s3_path):
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Total number of records are loading to Processed Table are:', f_df.count())
    f_df.coalesce(10).write.format("parquet").mode('overwrite').save(target_s3_path)
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')

def data_processing(processed_table):
    temp_location=processed_base_s3path+'tmp/'
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Writing Data to Temp location : {temp_location+processed_table}')
    write_data_to_processed(globals()[f"{processed_table}_df"],temp_location+processed_table)
    temp_df=spark_reader(temp_location,processed_table)
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Writing Data to Processed location : {processed_s3path+processed_table}')
    write_data_to_processed(temp_df,processed_s3path+processed_table)
    create_processed_table(pipeline_id,temp_df,processed_s3path+processed_table, app_name, domain_name,aws_region, env)
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Data Processing - Finished')
    
   
def create_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name,aws_region, env):
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Started')
    athena_util.AthenaUtil.create_athena_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name, aws_region, env)
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Finished')

def fee_cd_model_runner(): 
    try:
        print(f'fee_cd_model_runner - pipeline id:{pipeline_id} - Setting initial job status - Started')
        exec_id = job_status.put_jobstatus(inv_dict, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), env)
        print(f'fee_cd_model_runner - pipeline id:{pipeline_id} - Setting initial job status - Finished')
        print(f'fee_cd_model_runner - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Started')
        for domain_key,table_list_value in tables_list.items():
            base_path_string=globals()[f"{domain_key}_base_s3path"]
            print(f'domain key: {domain_key}, base_path :{base_path_string}')
            final_table_reader(base_path_string,table_list_value) 

        df_list = ['fee_sat','fee_cd']
        for table in df_list:
            print(f'fee_cd_model_runner - pipeline id:{pipeline_id} - read {f"{table}_query"} using TempView')           
            globals()[f"{table}_df"] = spark.sql(globals()[f"{table}_query"]).distinct()
            print(f'fee_cd_model_runner - pipeline id:{pipeline_id} - {table} record count: {globals()[f"{table}_df"].count()}')
         
            print(f'create new column fee_hub_hash using md5(FEE_ID) and md5(hashcolumns)')
            globals()[f"{table}_df"] = globals()[f"{table}_df"].withColumn('fee_hub_hash',md5('FEE_ID')).withColumn('diff_hash',md5(concat_ws(",",*hash_columns)))
            print(f'fee_cd_model_runner - pipeline id:{pipeline_id} - {f"{table}_df"} record count: {globals()[f"{table}_df"].count()}')

        print(f'get union dataframes between fee_sat and fee_cd df')
        fee_cd_union_df = globals()["fee_sat_df"].select(fee_sat_sequence_columns).union(globals()["fee_cd_df"].select(fee_sat_sequence_columns))
        print(f'fee_cd_union_df values count after union: {fee_cd_union_df.count()}')

        globals()[f"{table_name}_df"] = fee_cd_union_df.sort(fee_cd_union_df['fee_id'])

        data_processing(table_name)
       
        job_end_time = datetime.now(timezone('US/Eastern'))
        print('job_start_time and job_end_time dynamodb entry: ', str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
              str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")))
        print(f'fee_code_model_runner - pipeline id:{pipeline_id} - Updating job status - Started')
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.SUCCESS_FLAG, '', 0, 0, 0, sla_met)
        print(f'fee_code_model_runner - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'fee_code_model_runner - pipeline id:{pipeline_id} - Completed')
    except Exception as err:
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Failed with error: {err}')
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Updating job status - Started')
        job_end_time = datetime.now(timezone('US/Eastern'))
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.FAILURE_FLAG, str(err), 0, 0, 0, sla_met)
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Completed')
        raise SystemExit(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Failed with error: {err}')
 
if __name__ == "__main__":
    fee_cd_model_runner()
