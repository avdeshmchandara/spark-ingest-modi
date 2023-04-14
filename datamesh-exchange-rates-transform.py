from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from ingestion_framework.scripts import job_initializer
import sys
import boto3
from pytz import timezone
from datetime import datetime
from ingestion_framework.utils.dynamodb_util import JobStatus
from ingestion_framework.utils import athena_util, transformation_util, lineage_tracker_util
from ingestion_framework.constants import projectConstants

print("environment varialbe Initialization")
job_name = getResolvedOptions(sys.argv, ['JOB_NAME'])['JOB_NAME']
job_environment = getResolvedOptions(sys.argv, ['JobEnvironment'])['JobEnvironment']  # "dev"
app_name = getResolvedOptions(sys.argv, ['AppName'])['AppName']  # "fee_rpt"
aws_region = getResolvedOptions(sys.argv, ['AWSRegion'])['AWSRegion']
domain_name = getResolvedOptions(sys.argv, ['DomainName'])['DomainName']  # "finance"
sts_client = boto3.client(projectConstants.STS)
aws_account_id = sts_client.get_caller_identity()[projectConstants.ACCOUNT]
lineage_tracker_util.bulk_delete_dynamo_records(job_name, job_environment)

dynamodb = boto3.resource('dynamodb', region_name=aws_region)
dynamo_table_name = 'datamesh-jobstatus-{}'.format(job_environment)
table_name = 'cury_xcgh_rates'
pipeline_id = 'exchange_rates_transform_job'
inv_dict = {'pipeline_id': pipeline_id,
            'target_type': 's3',
            'target_file_name': table_name,
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

table_name = f"cury_xcgh_rates"
exchange_rates_prefix = f"data/processed/"
exchange_rates_s3_bucket = f"datamesh-{domain_name}-data-domain-{aws_account_id}-{job_environment}-{aws_region}"
final_base_s3path = f"s3://{exchange_rates_s3_bucket}/data/final/"
processed_s3path = f"s3a://{exchange_rates_s3_bucket}" + "/" + exchange_rates_prefix


def spark_reader(base_s3path, table_name):
    print("exchange-rates-transform - write_data_to_processed - reading from dataframe")
    return spark.read.format("parquet").load(f"{base_s3path}{table_name}/")


def write_data_to_processed(f_df, target_s3_path):
    print(f'exchange-rates-transform - write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    print(
        f'exchange-rates-transform - write_data_to_processed - pipeline id:{pipeline_id} - Total number of records are loading to Processed Table are:',
        f_df.count())
    f_df.write.format("parquet").mode('overwrite').save(target_s3_path)
    print(f'exchange-rates-transform - write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')


def create_processed_table(df, data_path, app_name, domain_name, job_environment):
    """Creates table in processed layer from processed_parquet_s3path
    Args:
        df: dataframe holding data from processed_parquet_s3path
        data_path:  S3 processed path that holds data in parquet format
        app_name: Application Name
        domain_name: Domain Name
        job_environment: Environment running in
    """
    # Athena Boto3 Client
    print(f'datamesh-exchange-rates-transform - create_processed_table - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    pyspark_transformation.createTargetTable(pipeline_id, df, data_path, app_name, aws_region)
    print(f'datamesh-exchange-rates-transform - create_processed_table - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')


def exchange_rate_runner(base_s3path):
    globals()[f"df_{table_name}"] = spark_reader(base_s3path, table_name)
    print("exchange-rates-transform - exchange_rate_runner - writing to processed path")
    write_data_to_processed(globals()[f"df_{table_name}"], processed_s3path + table_name)
    print("exchange-rates-transform - exchange_rate_runner - write complete")
    print("exchange-rates-transform - exchange_rate_runner - creating processed table")
    create_processed_table(globals()[f"df_{table_name}"], processed_s3path + table_name, app_name, domain_name, job_environment)
    print("exchange-rates-transform - exchange_rate_runner - table created")

source_target_dict = {}
pyspark_transformation = transformation_util.TranformationUtil(logger, spark, source_target_dict, job_name,
                                                                   job_environment, domain_name)
exchange_rate_runner(final_base_s3path)