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
from pyspark.sql.functions import md5, concat_ws,row_number,dense_rank,lit,col,max,when,date_sub
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
generic_invc_aggr_tables = ['genrc_invc_aggr','invc_ratg']


pipeline_id = 'fee_rpt_transform_job'
inv_dict = {'pipeline_id': pipeline_id, 
'target_type': 's3', 
'target_file_name': 'genrc_invc_aggr',
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
fee_rpt_ratings_s3_bucket= f"datamesh-ratings-data-domain-{aws_account_id}-{env}-{aws_region}"
fee_rpt_cross_s3_bucket= f"datamesh-cross-data-domain-{aws_account_id}-{env}-{aws_region}"
fee_rpt_oem_s3_bucket= f"datamesh-oem-data-domain-{aws_account_id}-{env}-{aws_region}"
fee_rpt_reference_s3_bucket= f"datamesh-reference-data-domain-{aws_account_id}-{env}-{aws_region}"
#Final Base Paths
finance_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/final/"
processed_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/processed/"
ratings_base_s3path = f"s3://{fee_rpt_ratings_s3_bucket}/data/final/"
cross_base_s3path = f"s3://{fee_rpt_cross_s3_bucket}/data/final/"
oem_base_s3path = f"s3://{fee_rpt_oem_s3_bucket}/data/final/"
reference_base_s3path = f"s3://{fee_rpt_reference_s3_bucket}/data/processed/"
#Processed S3 Path
processed_s3path = f"s3a://{fee_rpt_finance_s3_bucket}"+"/"+fee_rpt_prefix+"/"
index=0

source_tables_list = {
    "processed":["invc_hdr","invc_line_info","invc_line_dtl", "fee_sat"],
    "finance":["long_fee_typ","fee_info"],
    "cross":["lookup_dept","lookup_business_unit"],
    "oem":["entity_master_detail"],
    "reference":["country"],
    "ratings":["erp_transform_flow","ccdr_inst_id","ccdr_inst_rating","cdb_lkp_ratg_clss_num","cdb_lkp_eval_typ_cd","ccdr_org_ratg_obj","ccdr_org_rating","ccdr_prgm_id","ccdr_prgm_rating","ccdr_mdy_ratg_obj","edw_elkp_ratg_class_attrib","edw_instrument_rating","edw_instrument_organization","edw_program_rating","edw_program_organization","edw_organization_rating"]
    }

with open('orp_generic_inv_aggr.sql', "r") as f:
    f_orp_rating_cr_ncr_flag_query = f.read().format(env=env)
    print("read complete orp_rating_cr_ncr_flag sql")

with open('generic_inv_aggr.sql', "r") as f:
    f_generic_billing_report_query = f.read().format(env=env)
    print("read complete generic_billing_report_query sql")


with open('phx_generic_inv_aggr.sql', "r") as f:
    f_pheonix_rating_cr_ncr_flag_query = f.read().format(env=env)
    print("read complete phoenix_cr_ncr_determination sql")

print("end of environment varialbe Initialization")

def spark_reader(base_s3path,table):
    return spark.read.format("parquet").load(f"{base_s3path}{table}/")
def temp_view_creator(table_df,table):
    table_df.createOrReplaceTempView(table)
def final_table_reader(base_s3path,source_tables_list):
    for table in source_tables_list:
        print(f'base s3 path is: {base_s3path}')
        globals()[f"df_{table}"]=spark_reader(base_s3path,table)
        temp_view_creator(globals()[f"df_{table}"],table)
def write_data_to_processed(f_df, target_s3_path):
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Total number of records are loading to Processed Table are:', f_df.count())
    f_df.coalesce(10).write.format("parquet").mode('overwrite').save(target_s3_path)
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')
def data_processing(processed_table):
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Data Processing - Started')
    
    print("total number of records are: {}".format(globals()[f"{processed_table}_df"].count()))
    print("data frame schema is: {}".format(globals()[f"{processed_table}_df"].printSchema()) )
    print(globals()[f"{processed_table}_df"].show(5,truncate=False))
    print("path to write file: {}".format(processed_s3path+processed_table+"/"))
    write_data_to_processed(globals()[f"{processed_table}_df"],processed_s3path+processed_table+"/")
    create_processed_table(pipeline_id,globals()[f"{processed_table}_df"], processed_s3path+processed_table+"/", app_name, domain_name,aws_region, env)
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Data Processing - Finished')

   
def create_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name,aws_region, env):
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Started')
    athena_util.AthenaUtil.create_athena_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name, aws_region, env)
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Finished')

def fee_rpt_app_cr_ncr_flag_runner():

    try:
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Setting initial job status - Started')
        exec_id = job_status.put_jobstatus(inv_dict, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), env)
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Setting initial job status - Finished')
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Started')
        for domain_key,table_list_value in source_tables_list.items():
            base_path_string=globals()[f"{domain_key}_base_s3path"]
            print(f'domain key: {domain_key}, base_path :{base_path_string}')
            final_table_reader(base_path_string,table_list_value)
        
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Finished')
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - read f_orp_rating_cr_ncr_flag_query using TempView')   

        print(f'Generic billing report - 1')
        generic_billing_df = spark.sql(f_generic_billing_report_query)
        generic_billing_df.createOrReplaceTempView('generic_billing')

        
        orp_rating_df = spark.sql(f_orp_rating_cr_ncr_flag_query)
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - orp_rating_df record count: {orp_rating_df.count()}')

        phoenix_rating_df = spark.sql(f_pheonix_rating_cr_ncr_flag_query)
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - phoenix_rating_df record count: {phoenix_rating_df.count()}')

        union_orp_phoenix_df = orp_rating_df.unionAll(phoenix_rating_df)
        temp_orp_phoenix_df = union_orp_phoenix_df.select([col('invc_line_dtl_id'), col('RATIFER_ID'), col('load_dt')]).distinct()
        union_rating_df = union_orp_phoenix_df.drop('RATIFER_ID').distinct()

        invc_ratg_df = temp_orp_phoenix_df.alias('a').join(globals()["df_erp_transform_flow"].alias('b'), [col('b.ratifer_num') == col('a.RATIFER_ID')], 'left').select([col('a.invc_line_dtl_id'), col('a.RATIFER_ID').alias('RATFR_ID'), col('b.rating_identifier').alias('ratg_id'), col('a.load_dt')]).distinct()
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - invc_ratg record count: {invc_ratg_df.count()}')

        for table_name in generic_invc_aggr_tables:
            globals()[f"{table_name}_df"] = union_rating_df if table_name == 'genrc_invc_aggr' else invc_ratg_df
            data_processing(table_name)
        
        job_end_time = datetime.now(timezone('US/Eastern'))
        print('job_start_time and job_end_time dynamodb entry: ', str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
              str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")))
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Updating job status - Started')
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.SUCCESS_FLAG, '', 0, 0, 0, sla_met)
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'fee_rpt_app_cr_ncr_flag_runner - pipeline id:{pipeline_id} - Completed')
    except Exception as err:
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Failed with error: {err}')
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Updating job status - Started')
        job_end_time = datetime.now(timezone('US/Eastern'))
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.FAILURE_FLAG, str(err), 0, 0, 0, sla_met)
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Completed')
        raise SystemExit(f'Fee Reporting Transform - pipeline id:{pipeline_id} - Failed with error: {err}')
 
if __name__ == "__main__":
#    Running the application
    fee_rpt_app_cr_ncr_flag_runner()
