from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from ingestion_framework.scripts import job_initializer
import sys
import boto3
from pytz import timezone
from datetime import datetime, date
from ingestion_framework.utils.dynamodb_util import JobStatus
from ingestion_framework.utils import athena_util
from ingestion_framework.constants import projectConstants
from pyspark.sql.functions import date_format, to_date, current_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType
from functools import reduce

print("environment varialbe Initialization")
env = getResolvedOptions(sys.argv, ['JobEnvironment'])['JobEnvironment']  # "dev"
app_name = getResolvedOptions(sys.argv, ['AppName'])['AppName']  # "rating"
aws_region = getResolvedOptions(sys.argv, ['AWSRegion'])['AWSRegion']
domain_name = getResolvedOptions(sys.argv, ['DomainName'])['DomainName']  # "finance"
sts_client = boto3.client(projectConstants.STS)
aws_account_id = sts_client.get_caller_identity()[projectConstants.ACCOUNT]


dynamodb = boto3.resource('dynamodb', region_name=aws_region)
dynamo_table_name = 'datamesh-jobstatus-{}'.format(env)
table_name = 'fact_instrument_rating'
pipeline_id = 'rating_transform_job'
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

today_dt = date.today().strftime("%Y-%m-%d")
rating_prefix = f"data/processed/"
rating_s3_bucket = f"datamesh-{domain_name}-data-domain-{aws_account_id}-{env}-{aws_region}"
final_base_s3path = f"s3://{rating_s3_bucket}/data/final/"
processed_s3path = f"s3://{rating_s3_bucket}" + "/" + rating_prefix
combined_df_list = []
processed_table = "consolidate_ratg_transaction"

temporary_tables_list = ["instrument_rating", "program_rating", "sale_rating", "organization_rating", "tranche_rating", "maturity_rating"]

final_tables_list = ["fact_instrument_rating", "dim_instrument", "fact_program_rating", "rating_class_attribute",
                    "dim_program", "fact_sale_rating", "fact_organization_rating", "fact_tranche_rating",
                    "rel_deal_pool_tranche", "dim_deal", "dim_tranche", "entity_organization","fact_maturity_rating"]

print("end of environment varialbe Initialization")

with open('instrument_rating.sql', "r") as f:
    f_instrument_rating_query = f.read().format(env=env)
    print("read complete instrument_rating_query")
with open('program_rating.sql', "r") as f:
    f_program_rating_query = f.read().format(env=env)
    print("read complete program_rating_query")
with open('sale_rating.sql', "r") as f:
    f_sale_rating_query = f.read().format(env=env)
    print("read complete sale_rating_query")
with open('organization_rating.sql', "r") as f:
    f_organization_rating_query = f.read().format(env=env)
    print("read complete organization_rating_query")
with open('tranche_rating.sql', "r") as f:
    f_tranche_rating_query = f.read().format(env=env)
    print("read complete tranche_rating_query")
with open('maturity_rating.sql', "r") as f:
    f_maturity_rating_query = f.read().format(env=env)
    print("read complete maturity_rating_query")



# f_instrument_rating_query = f"""
#     select DISTINCT
# 			DI.ISSUER_NUM AS ORG_ID,
# 			R.INSTRUMENT_ID AS INSTR_ID,
# 			R.RATING_OBJECT_ID AS RATFR_ID,
#             CAST (DI.ISSUER_NUM AS VARCHAR(9) )|| 'D'
#               || CAST(R.INSTRUMENT_ID AS VARCHAR(9) )|| 'R'
#               || CAST(R.RATING_OBJECT_ID AS VARCHAR(9)) AS UNIQ_RATG_ID,
#             'INSTRUMENT' AS ENTITY_TYPE_CD,
#             NULL AS DEAL_ID,
#             R.RATING_TXT AS RATG_TXT,
#             R.RATING_DTM as RATG_DTM,
#             MIN(R.RATING_DTM) OVER(PARTITION BY R.INSTRUMENT_ID, R.RATING_OBJECT_ID, R.RATING_CLASS_CD) AS INIT_RATG_DTM,
#             R.RATING_CLASS_CD AS RATG_CLASS_CD,
#             R.PUBLISH_IND AS PUBL_INDC,
#             R.MONITOR_IND AS MNTR_INDC,
#             R.RATING_OFFICE_ID AS RATG_MOODYS_OFFC_ID,
#             R.RATING_OFFICE_NM AS RATG_MOODYS_OFFC_NM,
# 			RC.EVALUATION_TYPE_SHORT_DESC AS EVAL_SHORT_DESC_TXT,
#             RC.EVALUATION_TYPE_DESC AS EVAL_DESC_TXT
#     FROM
#         FACT_INSTRUMENT_RATING R
#         LEFT JOIN
#         RATING_CLASS_ATTRIBUTE RC
#         ON R.RATING_CLASS_CD = RC.RATING_CLASS_CD
#         LEFT JOIN
#         DIM_INSTRUMENT DI
#         ON
#         DI.INSTRUMENT_ID =  R.INSTRUMENT_ID
#         AND CAST(DI.END_DTM AS DATE) = cast('9999-12-31' as date)
#         AND cast(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  > DI.EFFT_DTM
#         AND cast(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  <= COALESCE(DI.TERM_DTM, cast('9999-12-31 23:59:59.999' as timestamp))
#     WHERE
#         CAST(r.EFFT_DTM AS DATE) <  DATE_TRUNC('year',CURRENT_TIMESTAMP)
#         AND r.END_DTM = cast('9999-12-31 00:00:00.000' as timestamp)
# """
#
# f_program_rating_query = f"""
#     select DISTINCT
# 			DI.ISSUER_NUM AS ORG_ID,
# 			R.PROGRAM_ID AS INSTR_ID,
#             R.RATING_OBJECT_ID AS RATFR_ID,
#             CAST (DI.ISSUER_NUM AS VARCHAR(9) )|| 'D'
#               || CAST(R.PROGRAM_ID AS VARCHAR(9) )|| 'R'
#               || CAST(R.RATING_OBJECT_ID AS VARCHAR(9)) AS UNIQ_RATG_ID,
#             'PROGRAM' AS ENTITY_TYPE_CD,
#             NULL AS DEAL_ID,
#             R.RATING_TXT AS RATG_TXT,
#             R.RATING_DTM as RATG_DTM,
#             MIN(R.RATING_DTM) OVER(PARTITION BY R.PROGRAM_ID, R.RATING_OBJECT_ID, R.RATING_CLASS_CD) AS INIT_RATG_DTM,
#             R.RATING_CLASS_CD AS RATG_CLASS_CD,
#             R.PUBLISH_IND AS PUBL_INDC,
#             R.MONITOR_IND AS MNTR_INDC,
#             R.RATING_OFFICE_ID AS RATG_MOODYS_OFFC_ID,
#             R.RATING_OFFICE_NM AS RATG_MOODYS_OFFC_NM,
# 			RC.EVALUATION_TYPE_SHORT_DESC AS EVAL_SHORT_DESC_TXT,
#             RC.EVALUATION_TYPE_DESC AS EVAL_DESC_TXT
#     FROM
#         FACT_PROGRAM_RATING R
#         LEFT JOIN
#         RATING_CLASS_ATTRIBUTE RC
#         ON R.RATING_CLASS_CD = RC.RATING_CLASS_CD
#         LEFT JOIN
#         DIM_PROGRAM DI
#         ON
#         DI.PROGRAM_ID =  R.PROGRAM_ID
#         AND CAST(DI.END_DTM AS DATE) = cast('9999-12-31' as date)
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  > DI.EFFT_DTM
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  <= COALESCE(DI.TERM_DTM, cast('9999-12-31 23:59:59.999' as timestamp))
#     WHERE
#         CAST(r.EFFT_DTM AS DATE) <  DATE_TRUNC('year',CURRENT_TIMESTAMP)
#         AND r.END_DTM = cast('9999-12-31 00:00:00.000' as timestamp)
# """
#
# f_sale_rating_query = f"""
#     select DISTINCT
#             EO.ORGANIZATION_ID AS ORG_ID,
# 			R.SALE_ID AS INSTR_ID,
# 			R.RATING_OBJECT_ID AS RATFR_ID,
#             CAST (EO.ORGANIZATION_ID AS VARCHAR(9) )|| 'D'
#               || CAST(R.SALE_ID AS VARCHAR(9) )|| 'R'
#               || CAST(R.RATING_OBJECT_ID AS VARCHAR(9)) AS UNIQ_RATG_ID,
#             'SALE' AS ENTITY_TYPE_CD,
#             NULL AS DEAL_ID,
#             R.RATING_TXT AS RATG_TXT,
#             R.RATING_DTM as RATG_DTM,
#             MIN(R.RATING_DTM) OVER(PARTITION BY R.SALE_ID, R.RATING_OBJECT_ID, R.RATING_CLASS_CD, EO.ORGANIZATION_ID) AS INIT_RATG_DTM,
#             R.RATING_CLASS_CD AS RATG_CLASS_CD,
#             R.PUBLISH_IND AS PUBL_INDC,
#             R.MONITOR_IND AS MNTR_INDC,
#             R.RATING_OFFICE_ID AS RATG_MOODYS_OFFICE_IND,
#             R.RATING_OFFICE_NM AS RATG_MOODYS_OFFICE_NM,
#             RC.EVALUATION_TYPE_SHORT_DESC AS EVAL_SHORT_DESC_TXT,
# 			RC.EVALUATION_TYPE_DESC AS EVAL_DESC_TXT
#     FROM
#         FACT_SALE_RATING R
#         LEFT JOIN
#         RATING_CLASS_ATTRIBUTE RC
#         ON R.RATING_CLASS_CD = RC.RATING_CLASS_CD
#         LEFT JOIN
#         ENTITY_ORGANIZATION EO
#         ON
#         R.SALE_ID = EO.ENTITY_ID
#         AND CAST(EO.END_DTM AS DATE) = cast('9999-12-31' as date)
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  > EO.EFFT_DTM
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  <= COALESCE(EO.TERM_DTM, cast('9999-12-31 23:59:59.999' as timestamp))
#      WHERE
#         EO.ORGANIZATION_ROLE_CD = 129
#         AND CAST(r.EFFT_DTM AS DATE) <  DATE_TRUNC('year',CURRENT_TIMESTAMP)
#         AND r.END_DTM = cast('9999-12-31 00:00:00.000' as timestamp)
#  """
#
# f_organization_rating_query = f"""
#     select DISTINCT
# 			R.ORGANIZATION_ID AS ORG_ID,
# 			R.ORGANIZATION_ID AS INSTR_ID,
#             R.RATING_OBJECT_ID AS RATFR_ID,
#             CAST (R.ORGANIZATION_ID  AS VARCHAR(9) )|| 'D'
#               || CAST(R.ORGANIZATION_ID  AS VARCHAR(9) )|| 'R'
#               || CAST(R.RATING_OBJECT_ID AS VARCHAR(9)) AS UNIQ_RATG_ID,
#             'ORGANIZATION' AS ENTITY_TYPE_CD,
#             NULL AS DEAL_ID,
#             R.RATING_TXT AS RATG_TXT,
#             R.RATING_DTM as RATG_DTM,
#             MIN(R.RATING_DTM) OVER(PARTITION BY R.ORGANIZATION_ID, R.RATING_OBJECT_ID, R.RATING_CLASS_CD) AS INIT_RATG_DTM,
#             R.RATING_CLASS_CD AS RATG_CLASS_CD,
# 			R.PUBLISH_IND AS PUBL_INDC,
#             R.MONITOR_IND AS MNTR_INDC,
#             R.RATING_OFFICE_ID AS RATG_MOODYS_OFFC_ID,
#             R.RATING_OFFICE_NM AS RATG_MOODYS_OFFC_NM,
# 			RC.EVALUATION_TYPE_SHORT_DESC AS EVAL_SHORT_DESC_TXT,
#             RC.EVALUATION_TYPE_DESC AS EVAL_DESC_TXT
#         FROM
#         FACT_ORGANIZATION_RATING R
#         LEFT JOIN
#         RATING_CLASS_ATTRIBUTE RC
#         ON R.RATING_CLASS_CD = RC.RATING_CLASS_CD
#     WHERE
#         CAST(R.EFFT_DTM AS DATE) <  DATE_TRUNC('year',CURRENT_TIMESTAMP)
#         AND R.END_DTM = cast('9999-12-31 12:00:00.000' as timestamp)
# """
#
# f_tranche_rating_query = f"""
#     select DISTINCT
# 			DI.ISSUER_NUM AS ORG_ID,
# 			R.TRANCHE_ID AS INSTR_ID,
#             R.RATING_OBJECT_ID AS RATFR_ID,
#             CAST (DI.ISSUER_NUM AS VARCHAR(9) )|| 'D'
#               || CAST(R.TRANCHE_ID AS VARCHAR(9) )|| 'R'
#               || CAST(R.RATING_OBJECT_ID AS VARCHAR(9)) AS UNIQ_RATG_ID,
#             'TRANCHE' AS ENTITY_TYPE_CD,
#             dpt0.DEAL_ID AS DEAL_ID,
#             R.RATING_TXT AS RATG_TXT,
#             R.RATING_DTM as RATG_DTM,
#             MIN(R.RATING_DTM) OVER(PARTITION BY R.TRANCHE_ID, R.RATING_OBJECT_ID, R.RATING_CLASS_CD) AS INIT_RATG_DTM,
#             R.RATING_CLASS_CD AS RATG_CLASS_CD,
#             R.PUBLISH_IND AS PUBL_INDC,
#             R.MONITOR_IND AS MNTR_INDC,
#             R.RATING_OFFICE_ID AS RATG_MOODYS_OFFC_ID,
#             R.RATING_OFFICE_NM AS RATG_MOODYS_OFFC_NM,
# 			RC.EVALUATION_TYPE_SHORT_DESC AS EVAL_SHORT_DESC_TXT,
#             RC.EVALUATION_TYPE_DESC AS EVAL_DESC_TXT
#         FROM
#         FACT_TRANCHE_RATING R
#         LEFT JOIN
#         RATING_CLASS_ATTRIBUTE RC
#         ON R.RATING_CLASS_CD = RC.RATING_CLASS_CD
#         LEFT JOIN
#         REL_DEAL_POOL_TRANCHE dpt0
#         on dpt0.TRANCHE_ID = R.TRANCHE_ID
#         AND dpt0.RECORD_TYPE_CD = 'DEAL_POOL_TRANCHE'
#         LEFT JOIN DIM_DEAL dl
#         ON dpt0.DEAL_ID = dl.DEAL_ID
# 		and cast(dl.END_DTM as date) = cast('9999-12-31' as date)
#         AND dl.STATUS_CD NOT IN ('151242', '151245')
#         LEFT JOIN
#         DIM_TRANCHE DI
#         ON
#         DI.TRANCHE_ID =  R.TRANCHE_ID
#         and cast(dl.END_DTM as date) = cast('9999-12-31' as date)
#         AND cast(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  > DI.EFFT_DTM
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  <= COALESCE(DI.TERM_DTM, cast('9999-12-31 23:59:59.999' as timestamp))
#      WHERE
#         CAST(r.EFFT_DTM AS DATE) <  DATE_TRUNC('year',CURRENT_TIMESTAMP)
#         AND r.END_DTM = cast('9999-12-31 00:00:00.000' as timestamp)
# """


# f_maturity_rating_query = f"""
# select DISTINCT
# 			EO.ORGANIZATION_ID AS ORG_ID,
# 			R.MATURITY_ID AS INSTR_ID,
#             R.RATING_OBJECT_ID AS RATFR_ID,
#             CAST (EO.ORGANIZATION_ID AS VARCHAR(9) )|| 'D'
#               || CAST(R.MATURITY_ID AS VARCHAR(9) )|| 'R'
#               || CAST(R.RATING_OBJECT_ID AS VARCHAR(9)) AS UNIQ_RATG_ID,
#             'MATURITY' AS ENTITY_TYPE_CD,
#             NULL AS DEAL_ID,
#             R.RATING_TXT AS RATG_TXT,
#             R.RATING_DTM as RATG_DTM,
#             MIN(R.RATING_DTM) OVER(PARTITION BY R.MATURITY_ID, R.RATING_OBJECT_ID, R.RATING_CLASS_CD, EO.ORGANIZATION_ID) AS INIT_RATG_DTM,
#             R.RATING_CLASS_CD AS RATG_CLASS_CD,
#             R.PUBLISH_IND AS PUBL_INDC,
#             R.MONITOR_IND AS MNTR_INDC,
#             R.RATING_OFFICE_ID AS RATG_MOODYS_OFFC_ID,
#             R.RATING_OFFICE_NM AS RATG_MOODYS_OFFC_NM,
# 			RC.EVALUATION_TYPE_SHORT_DESC AS EVAL_SHORT_DESC_TXT,
#             RC.EVALUATION_TYPE_DESC AS EVAL_DESC_TXT
#         FROM
#         FACT_MATURITY_RATING R
#         LEFT JOIN
#         RATING_CLASS_ATTRIBUTE RC
#         ON R.RATING_CLASS_CD = RC.RATING_CLASS_CD
#         LEFT JOIN
#         ENTITY_ORGANIZATION EO
#         ON
#         R.MATURITY_ID = EO.ENTITY_ID
#         AND CAST(EO.END_DTM AS DATE) = cast('9999-12-31' as date)
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  > EO.EFFT_DTM
#         AND CAST(DATE_TRUNC('year',CURRENT_TIMESTAMP) as timestamp)  <= COALESCE(EO.EFFT_DTM, cast('9999-12-31 23:59:59.999' as timestamp))
#      WHERE
#         EO.ORGANIZATION_ROLE_CD = 129
#         AND CAST(r.EFFT_DTM AS DATE) <  DATE_TRUNC('year',CURRENT_TIMESTAMP)
#         AND r.END_DTM = cast('9999-12-31 00:00:00.000' as timestamp)
# """

def spark_reader(base_s3path, table):
    return spark.read.format("parquet").load(f"{base_s3path}{table}/")


def temp_view_creator(table_df, table):
    table_df.createOrReplaceTempView(table)


def final_table_reader(base_s3path, tables_list):
    for table in tables_list:
        globals()[f"df_{table}"] = spark_reader(base_s3path, table)
        temp_view_creator(globals()[f"df_{table}"], table)
        print(f'print the table list:{table} - data load complete')


def write_data_to_processed(f_df, target_s3_path):
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    print(
        f'write_data_to_processed - pipeline id:{pipeline_id} - Total number of records are loading to Processed Table are:',
        f_df.count())
    f_df.coalesce(10).write.format("parquet").mode('overwrite').save(target_s3_path)
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')


def initial_data_processing(processed_s3path, temporary_table):
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Initial Data Processing - Started')
    src_df = spark.sql(globals()[f"f_{temporary_table}_query"])
    return src_df


def create_processed_table(df, data_path, app_name, domain_name, env):
    """Creates table in processed layer from processed_parquet_s3path
    Args:
        df: dataframe holding data from processed_parquet_s3path
        data_path:  S3 processed path that holds data in parquet format
        app_name: Application Name
        domain_name: Domain Name
        env: Environment running in
    """
    # Athena Boto3 Client
    print(f'create_processed_table - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    athena_client = boto3.client('athena', region_name='us-east-1')
    col_info = df.dtypes
    s3_path = data_path[5:]
    bucket_name = s3_path.split('/')[0]
    db_name = f"datamesh_{domain_name}_processed_{env}"
    work_grp = f"datamesh-{app_name}-{env}"
    table_name = s3_path.split('/')[-1]
    if len(table_name) < 1:
        table_name = s3_path.split('/')[-2]
    out_loc = "s3://{}/logs/athena/{}/".format(bucket_name, table_name)
    context = {'Database': db_name}
    q_1 = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{table_name}`
         (
         """
    q2 = """"""
    for col in col_info:
        tmp = "`{}`   {},\n".format(col[0], col[1])
        q2 = q2 + tmp
    q_2 = q2[:-2]
    q_3 = """)
    STORED AS PARQUET
    LOCATION '{}'
    """.format(data_path)
    create_qry = q_1 + q_2 + q_3
    drp_qry = f"DROP TABLE IF EXISTS `{table_name}`"
    # athena_querys = [drp_qry, create_qry]  # TODO: Uncomment this code once IAM role is fixed
    athena_querys = [drp_qry, create_qry]

    for qry in athena_querys:
        print(f'create_processed_table - pipeline id:{pipeline_id} - Query to be executed is: ', qry)
        res = athena_client.start_query_execution(
            QueryString=qry,
            QueryExecutionContext=context,
            WorkGroup=work_grp,
            ResultConfiguration={'OutputLocation': out_loc})
        q_exec_id = res['QueryExecutionId']
        while True:
            status = athena_client.get_query_execution(QueryExecutionId=q_exec_id)
            state = status['QueryExecution']['Status']['State']
            if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                break
        if state == "FAILED":
            raise Exception(
                "Athena Table Creation Failed with Error :" + status['QueryExecution']['Status']['StateChangeReason'])
    print(f'create_processed_table - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')


def rating_app_transform_runner():
    """Runs all steps required for Phoenix Transform layer"""
    try:
        print(f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Setting initial job status - Started')
        exec_id = job_status.put_jobstatus(inv_dict, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), env)
        print(f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Setting initial job status - Finished')
        print(
            f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Started')
        final_table_reader(final_base_s3path, final_tables_list)
        print(
            f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Finished')
        for temporary_table in temporary_tables_list:
            print(
                f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Data Processing for Processed Table - {temporary_table} - Started')
            globals()[f"{temporary_table}_df"] = initial_data_processing(processed_s3path, temporary_table)
            combined_df_list.append(globals()[f"{temporary_table}_df"])
        print("union of all the dataframes")
        combined_df = reduce(DataFrame.unionAll, combined_df_list)
        combined_df = combined_df.withColumn('load_dt', date_format(current_timestamp(), 'yyyy-MM-dd').cast(DateType()))
        write_data_to_processed(combined_df, processed_s3path + processed_table)
        create_processed_table(combined_df, processed_s3path + processed_table, app_name, domain_name, env)
        job_end_time = datetime.now(timezone('US/Eastern'))
        print('job_start_time and job_end_time dynamodb entry: ', str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
              str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")))
        print(f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Updating job status - Started')
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE,
                                    str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
                                    str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.SUCCESS_FLAG, '',
                                    0, 0, 0, sla_met)
        print(f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'rating_app_transform_runner  - pipeline id:{pipeline_id} - Completed')

    except Exception as err:
        print(f'Rating Transform - pipeline id:{pipeline_id} - Failed with error: {err}')
        print(f'Rating Transform - pipeline id:{pipeline_id} - Updating job status - Started')
        job_end_time = datetime.now(timezone('US/Eastern'))
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE,
                                    str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
                                    str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.FAILURE_FLAG,
                                    str(err), 0, 0, 0, sla_met)
        print(f'Rating Transform - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'Rating Transform - pipeline id:{pipeline_id} - Completed')
        raise SystemExit(f'Rating Transform - pipeline id:{pipeline_id} - Failed with error: {err}')


if __name__ == "__main__":
    #    Running the application
    rating_app_transform_runner()
