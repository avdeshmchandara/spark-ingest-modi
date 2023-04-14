from awsglue.transforms import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from ingestion_framework.scripts import job_initializer
import sys
import boto3
from pytz import timezone
from datetime import datetime, timedelta, date
from ingestion_framework.utils.dynamodb_util import JobStatus
from ingestion_framework.utils import athena_util
from ingestion_framework.constants import projectConstants
from pyspark.sql.functions import md5, concat_ws, row_number, dense_rank, lit, col, max, when, date_sub

from pyspark.sql import Window
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

pipeline_id = 'fee_rpt_transform_job'
inv_dict = {'pipeline_id': pipeline_id,
            'target_type': 's3',
            'target_file_name': 'genrc_invc_aggr',
            'load_frequency': 'daily',
            'load_type': 'full',
            'domain_name': domain_name,
            'load_phase': 'processed'}
# Initialize spark and logger
spark = job_initializer.initializeSpark(app_name)
logger = job_initializer.getGlueLogger(spark)
job_status = JobStatus(spark, logger)
job_start_time = datetime.now(timezone('US/Eastern'))
exec_id = ""
sla_met = ""
today_dt = date.today().strftime("%Y-%m-%d")
fee_rpt_prefix = f"data/processed/"
# S3 Buckets

fee_rpt_finance_s3_bucket = f"datamesh-finance-data-domain-{aws_account_id}-{env}-{aws_region}"
fee_rpt_ratings_s3_bucket = f"datamesh-ratings-data-domain-{aws_account_id}-{env}-{aws_region}"

# Final Base Paths
finance_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/final/"
processed_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/processed/"
raw_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/landing/"
ratings_base_s3path = f"s3://{fee_rpt_ratings_s3_bucket}/data/final/"

# Processed S3 Path
processed_s3path = f"s3a://{fee_rpt_finance_s3_bucket}" + "/" + fee_rpt_prefix + "/"

index = 0

source_tables_list = {
    "processed": ["invc_hdr", "invc_line_info", "invc_line_dtl", "fee_sat"],
    "finance": ["regl_fee_schd"]
}
t_table_name = 'regl_fee_schd'

hash_columns=[ "regl_fee_schd_nm", "regl_fee_prgm"]
select_columns = [ "invc_line_dtl_id",  "regl_fee_schd_nm", "regl_fee_prgm", "diff_hash", "load_dtm"]
chk_dup_cols = [ "invc_line_dtl_id",  "regl_fee_schd_nm", "regl_fee_prgm", "diff_hash"]

with open('query_orp_phx.sql', "r") as f:
    f_query_orp_phx = f.read().format(env=env)

def spark_reader(base_s3path, table):
    return spark.read.format("parquet").load(f"{base_s3path}{table}/")

def temp_view_creator(table_df, table):
    table_df.createOrReplaceTempView(table)

def final_table_reader(base_s3path, source_tables_list):
    for table in source_tables_list:
        print(f'In final_table_reader base s3 path is: {base_s3path}       created dataframe:  df_{table}')
        globals()[f"df_{table}"] = spark_reader(base_s3path, table)
        temp_view_creator(globals()[f"df_{table}"], table)

def write_data_to_processed(f_df, target_s3_path):
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    print(f'number of records are loaded: ', f_df.count())
    f_df.coalesce(1).write.format("parquet").mode('overwrite').save(target_s3_path)
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')

def data_processing(processed_df, processed_table):
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Data Processing - Started')
    print("total number of records are: {}".format(processed_df.count()))
    print("data frame schema is: {}".format(processed_df.printSchema()))
    print(processed_df.show(5, truncate=False))
    print("path to write file: {}".format(processed_s3path + processed_table + "/"))
    write_data_to_processed(processed_df, processed_s3path + processed_table + "/")
    create_processed_table(pipeline_id, processed_df, processed_s3path + processed_table + "/",
                           app_name, domain_name, aws_region, env)
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Data Processing - Finished')


def create_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name, aws_region, env):
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Started')
    athena_util.AthenaUtil.create_athena_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name,
                                                         aws_region, env)
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Finished')

def is_s3_path_empty(s3_bucket,folder_name):
    s3 = boto3.client('s3')
    try:
        count = s3.list_objects_v2(Bucket=s3_bucket, Prefix=folder_name, Delimiter="/")
        if 'Contents' not in count:
            return True
        else:
            return False
    except Exception as e:
        raise SystemExit(f'is_s3_path_empty check - Failed with error: {e}')

def fee_rpt_regl_fee_schd_runner():
    try:
        print(f'fee_rpt_regl_fee_schd_runner - pipeline id:{pipeline_id} - Setting initial job status - Started')
        exec_id = job_status.put_jobstatus(inv_dict, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), env)
        print(f'fee_rpt_regl_fee_schd_runner - pipeline id:{pipeline_id} - Setting initial job status - Finished')
        for domain_key, table_list_value in source_tables_list.items():
            base_path_string = globals()[f"{domain_key}_base_s3path"]
            print(f'domain key: {domain_key}, base_path :{base_path_string}')
            final_table_reader(base_path_string, table_list_value)

        src_records = spark.sql(f_query_orp_phx)\
            .withColumn('diff_hash', md5(concat_ws(",", *hash_columns)))\
            .withColumn('load_dtm', lit(str(datetime.now(timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S"))) )\
            .select(*select_columns).distinct()

        globals()[f"{t_table_name}_df"] = src_records

        folder_name = fee_rpt_prefix + t_table_name + "/"
        if not is_s3_path_empty(fee_rpt_finance_s3_bucket, folder_name):
            tgt_records = spark_reader(processed_s3path, t_table_name).select(*select_columns).cache()
            new_records_df = src_records.join(tgt_records, on='invc_line_dtl_id', how='leftanti')
            upd_cond = [src_records.invc_line_dtl_id == tgt_records.invc_line_dtl_id , src_records.diff_hash != tgt_records.diff_hash ]
            upd_records_df = src_records.join(tgt_records, on=upd_cond, how='leftsemi')
            globals()[f"{t_table_name}_df"] = tgt_records.union(new_records_df)\
                                                         .union(upd_records_df)\
                                                         .select(*select_columns).distinct()

        globals()[f"{t_table_name}_df"] = globals()[f"{t_table_name}_df"].dropDuplicates(chk_dup_cols)
        data_processing(globals()[f"{t_table_name}_df"], t_table_name)

        job_end_time = datetime.now(timezone('US/Eastern'))
        print('job_start_time and job_end_time dynamodb entry: ', str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
              str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")))
        print(f'fee_rpt_regl_fee_schd_runner - pipeline id:{pipeline_id} - Updating job status - Started')
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE,
                                    str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
                                    str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.SUCCESS_FLAG, '',
                                    0, 0, 0, sla_met)
        print(f'fee_rpt_regl_fee_schd_runner - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'fee_rpt_regl_fee_schd_runner - pipeline id:{pipeline_id} - Completed')
    except Exception as err:
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Failed with error: {err}')
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Updating job status - Started')
        job_end_time = datetime.now(timezone('US/Eastern'))
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE,
                                    str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
                                    str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.FAILURE_FLAG,
                                    str(err), 0, 0, 0, sla_met)
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Completed')
        raise SystemExit(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Failed with error: {err}')


if __name__ == "__main__":
    #    Running the application
    fee_rpt_regl_fee_schd_runner()