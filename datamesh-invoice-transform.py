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
table_name='invoice'
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
fee_rpt_finance_s3_bucket=f"datamesh-{domain_name}-data-domain-{aws_account_id}-{env}-{aws_region}"
fee_rpt_reference_s3_bucket=f"datamesh-reference-data-domain-{aws_account_id}-{env}-{aws_region}"

#Final Base Paths
finance_base_s3path = f"s3://{fee_rpt_finance_s3_bucket}/data/final/"
reference_base_s3path = f"s3://{fee_rpt_reference_s3_bucket}/data/final/"

#Processed S3 Path
processed_s3path = f"s3a://{fee_rpt_finance_s3_bucket}"+"/"+fee_rpt_prefix

index=0

processed_tables_list = ["invc_hdr", "invc_line_info", "invc_line_dtl"]
raw_tables_list = {
    "finance":["adjustments", "customer", "family_info", "fee_info", "inv_seq_customer","invoice_seq_main", "invoice",
     "line_description", "line_item", "lookup_template","map_cury_codes", "phx_ps_staging","bill_fam", "bill_itm_addr", 
     "bill_itm_ln_dtl", "bill_itm_ln", "bill_itm","fee_logic", "fee_sched", "lkp_bill_itm_status", "long_fee_typ", "prcng_ln",
     "pyramid_invoice","pyramid_pricing_type","pyramid_invoice_party_role_contact_person_role","pyramid_commercial_customer_version",
     "pyramid_commercial_customer","pyramid_party_role","pyramid_party","pyramid_organization","pyramid_invoice_line_item",
     "pyramid_fee_schedule_fee_type","pyramid_fee_schedule","pyramid_billable_activity","pyramid_ratable_entity"],
    "reference":["lookup_ps_bus_unit_tbl_ar"]
    }

md5_hash_columns_to_skip=['start_dt','end_dt']

key_columns={
    "invc_hdr":"SRC_SYS_UNIQ_KEY_TXT",
    "invc_line_info":"SRC_SYS_UNIQ_KEY_TXT",
    "invc_line_dtl":"SRC_SYS_UNIQ_KEY_TXT"
}

sequence_columns={
    "invc_hdr":"INVC_HDR_ID",
    "invc_line_info":"INVC_LINE_INFO_ID",
    "invc_line_dtl":"INVC_LINE_DTL_ID"
}

join_columns={
    "join1":{"leftTable":['src_bill_id','src_bill_vrsn_id'],"rightTable":['src_bill_id','src_bill_vrsn_id']},
    "join2":{"leftTable":['src_bill_id','SRC_BILL_LINE_SEQ_NUM','src_bill_vrsn_id'],"rightTable":['src_bill_id','SRC_BILL_LINE_SEQ_NUM','src_bill_vrsn_id']}
}

with open('invc_hdr.sql', "r") as f:
    f_invc_hdr_query = f.read().format(env=env)
    print("read complete f_invc_hdr_query")
with open('invc_line_info.sql', "r") as f:
    f_invc_line_info_query = f.read().format(env=env)
    print("read complete f_invc_line_info_query")
with open('invc_line_dtl.sql', "r") as f:
    f_invc_line_dtl_query = f.read().format(env=env)
    print("read complete f_invc_line_dtl_query")

print("end of environment varialbe Initialization")


def spark_reader(base_s3path,table):
    return spark.read.format("parquet").load(f"{base_s3path}{table}/")

def temp_view_creator(table_df,table):
    table_df.createOrReplaceTempView(table)

def final_table_reader(base_s3path,tables_list):
    print(f" list of tables, base_s2path :  {tables_list}, {base_s3path}")
    list_of_dates = []
    s3=boto3.resource('s3')
    bucket_name = fee_rpt_finance_s3_bucket
    my_bucket = s3.Bucket(bucket_name)
    prefix_folder = 'data/error/'
    date_str = ''
    for object_summary in my_bucket.objects.filter(Prefix=str(prefix_folder)):
        date_path = object_summary.key.split('/')[2]
        dpp = date_path.split('=')[1]
        if date_path is not None and date_str != dpp:
            date_str = dpp
            list_of_dates.append(datetime.strptime(dpp, "%Y-%m-%d"))
    print(f"list of dates {list_of_dates} at final_table_reader")
    if len(list_of_dates) > 0:
        sorted_list = sorted(list_of_dates)
        max_date = sorted_list[-1]
        maxDateTime = str(max_date)
        max_date_str = maxDateTime[:10]
        bdq_path = f"s3://{bucket_name}/{prefix_folder}"
        bdq_err_path = f"{bdq_path}run_dt={max_date_str}/"
        print(f" bdq error path to readh from : {bdq_err_path}")
        bdq_err_df = spark.read.parquet(bdq_err_path)
        print(bdq_err_df.printSchema())
        for table in tables_list:
            table_df = spark_reader(base_s3path,table)
            table_lo_df = table_df.toDF(*[c.lower() for c in table_df.columns]).alias("tbl_df")
            bdq_err_tbl_df = bdq_err_df.filter(bdq_err_df.table_name == table)
            print(f" printing table name and table_name match")
            print(table)
            if not bdq_err_tbl_df.rdd.isEmpty():
                bdq_err_pk_dff = bdq_err_tbl_df.select("primary_keys").distinct()
                json_schema = spark.read.json(bdq_err_pk_dff.rdd.map(lambda row: row.primary_keys)).schema
                print(f" printing json_schema : {json_schema}")
                bdq_err_pk_dff2 = bdq_err_pk_dff.withColumn('parsed', F.from_json(col('primary_keys'), json_schema)).select("parsed.*").distinct().alias("bdq_df")
                bdq_err_pks_df_cols = bdq_err_pk_dff2.columns
                joined_table_df = table_lo_df.join(bdq_err_pk_dff2, bdq_err_pks_df_cols, 'left_anti')
                globals()[f"df_{table}"] = joined_table_df
                temp_view_creator(globals()[f"df_{table}"],table)
                print("temp_view_creator",temp_view_creator)
            else:
                globals()[f"df_{table}"] = spark_reader(base_s3path,table)
                temp_view_creator(globals()[f"df_{table}"],table)
    else:
        for table in tables_list:
            globals()[f"df_{table}"] = spark_reader(base_s3path,table)
            temp_view_creator(globals()[f"df_{table}"],table)

def write_data_to_processed(f_df, target_s3_path):
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Started')
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Total number of records are loading to Processed Table are:', f_df.count())
    f_df.coalesce(10).write.format("parquet").mode('overwrite').save(target_s3_path)
    print(f'write_data_to_processed - pipeline id:{pipeline_id} - Data Load Processed Layer - Finished')

def is_s3_path_empty(s3_bucket,folder_name):
    s3=boto3.client('s3')
    resp = s3.list_objects(Bucket=s3_bucket, Prefix=folder_name, Delimiter='/',MaxKeys=1)
    if "Contents" in resp:
        if resp["Contents"][0]["Size"] > 0:
            return False
        else:
            return True
    else:
        return True

def sequence_generator(df,processed_table,max_seq_num=0):
    return (df
    .withColumn(sequence_columns[processed_table],dense_rank().over(Window.orderBy(key_columns[processed_table])) + max_seq_num)
    .withColumn("start_dt",lit(date.today()).cast(DateType()))
    .withColumn("end_dt",lit("2999-12-31").cast(DateType()))
    )

def initial_data_processing(processed_s3path, processed_table):
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Initial Data Processing - Started')
    src_df=spark.sql(globals()[f"f_{processed_table}_query"])
    print(f'sequence_generator - pipeline id:{pipeline_id} - Sequence Value Generation - Started')
    globals()[f"{processed_table}_df"]=sequence_generator(src_df,processed_table)
    print(f'sequence_generator - pipeline id:{pipeline_id} - Sequence Value Generation - Ended')

    if globals()[index] > 0:
        parent_df=globals()[f"{processed_tables_list[globals()[index]-1]}_df"].alias("rightDf")
        globals()[f"{processed_table}_df"]=child_table_id_loader(globals()[f"{processed_table}_df"].alias("leftDf"),parent_df,processed_table)
    print("Sample data for table - {} ".format(processed_table))
    print(globals()[f"{processed_table}_df"].show(5,truncate=False))
    
    write_data_to_processed(globals()[f"{processed_table}_df"],processed_s3path+processed_table)
    create_processed_table(pipeline_id,globals()[f"{processed_table}_df"], processed_s3path+processed_table, app_name, domain_name,aws_region, env)
    print(f'initial_data_processing - pipeline id:{pipeline_id} - Initial Data Processing - Finished')

    
def child_table_id_loader(child_df,parent_df,processed_table):
    column_name=sequence_columns[processed_tables_list[globals()[index]-1]]
    parent_df=parent_df.select(*join_columns["join{}".format(globals()[index])]["rightTable"]+ [column_name])
    parent_df=parent_df.select([col(i).alias(f"{i}_rightdf") for i in parent_df.columns])
    return (child_df
    .join(parent_df,[child_df[f]==parent_df[f"{s}_rightdf"] for (f,s) in zip(join_columns["join{}".format(globals()[index])]["leftTable"],join_columns["join{}".format(globals()[index])]["rightTable"])],'left')
    .withColumn(column_name,col(f"{column_name}_rightdf"))
    .select(*child_df.columns).distinct()
    )

def historic_data_preservator(processed_s3path,processed_table):
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Historic Data Preservation - Started')
    #Source Data reader
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Source & Target Data Reader - Started')
    globals()[f"{processed_table}_src_df"]=spark.sql(globals()[f"f_{processed_table}_query"])

    # target data reader
    globals()[f"{processed_table}_tgt_df"]=spark_reader(processed_s3path, processed_table).cache()

    source_table_columns=globals()[f"{processed_table}_src_df"].columns
    target_table_columns=globals()[f"{processed_table}_tgt_df"].columns

    print("source columns are: ",source_table_columns)
    print("target columns are: ",target_table_columns)
 
    #Identifying new columns
    new_column_list=[src_col for src_col in source_table_columns if src_col not in target_table_columns]
    print("additional columns from source df are: ",new_column_list)

    evolved_schema_colmns_list=[]
    for x in globals()[f"{processed_table}_src_df"].schema.fields:
        if x.name in new_column_list:
            evolved_schema_colmns_list.append(lit(None).cast(x.dataType).alias(x.name))
        else:
            evolved_schema_colmns_list.append(col(x.name).cast(x.dataType).alias(x.name))
    print(evolved_schema_colmns_list)

    globals()[f"{processed_table}_tgt_df"]=globals()[f"{processed_table}_tgt_df"].select(*evolved_schema_colmns_list + ["start_dt", "end_dt"])

    print(globals()[f"{processed_table}_tgt_df"].printSchema())

    print("target data is: ",globals()[f"{processed_table}_tgt_df"].show(10,truncate=False))
    print("target data post schema evolution is : ",globals()[f"{processed_table}_tgt_df"].show(10,truncate=False))

    # Identifying History Records
    globals()[f"{processed_table}_tgt_history_df"]=globals()[f"{processed_table}_tgt_df"].where("end_dt!='2999-12-31'")

    # Identifying Latest/Current Records
    globals()[f"{processed_table}_tgt_latest_df"]=globals()[f"{processed_table}_tgt_df"].where("end_dt='2999-12-31'")

    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Source & Target Data Reader - Finished')

    max_seq_num = globals()[f"{processed_table}_tgt_latest_df"].groupBy().max().collect()[0][0]
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Maximum Sequence Number for Target Table - {processed_table} is  - {max_seq_num}')
#   identifying columns that are not reqruied for hash value generation.
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - MD5 Hashing For Non Key Columns - Started')
    if globals()[index]>0:
        hash_cols=[x for x in globals()[f"{processed_table}_src_df"].columns if x not in [key_columns[processed_table]] + [sequence_columns[processed_table]] + md5_hash_columns_to_skip + [sequence_columns[processed_tables_list[globals()[index]-1]]]]
    else:
        hash_cols=[x for x in globals()[f"{processed_table}_src_df"].columns if x not in [key_columns[processed_table]] + [sequence_columns[processed_table]] + md5_hash_columns_to_skip]
#   hash value generation for all columns in source dataset
    globals()[f"{processed_table}_src_df"] = globals()[f"{processed_table}_src_df"].alias("src_df").withColumn("src_md5", md5(concat_ws("", *hash_cols))).cache()
#   hash value generation for all columns in source dataset
    globals()[f"{processed_table}_tgt_latest_df"] = globals()[f"{processed_table}_tgt_latest_df"].alias("tgt_df").withColumn("tgt_md5", md5(concat_ws("", *hash_cols))).cache()

    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Source Data Count is:',globals()[f"{processed_table}_src_df"].count())
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Total Target table record Count is:',globals()[f"{processed_table}_tgt_df"].count())
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Target Table - History Records Count is:',globals()[f"{processed_table}_tgt_history_df"].count())
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Target Table - Latest/Current Record Count is:',globals()[f"{processed_table}_tgt_latest_df"].count())


    print(f'historic_data_preservator - pipeline id:{pipeline_id} - MD5 Hashing For Non Key Columns - Finished')



#   Joining source and target dataframes on key column
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Joining Source and Target Tables For SCD - Started')
    join_df=globals()[f"{processed_table}_tgt_latest_df"].join(globals()[f"{processed_table}_src_df"],col(f"src_df.{key_columns[processed_table]}") == col(f"tgt_df.{key_columns[processed_table]}"),"full_outer")
    join_df=join_df.cache()
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Joining Source and Target Tables For SCD - Finished')

    insert_df=new_insert_record_identifier(join_df,processed_table,max_seq_num)
    print(f'new_insert_record_identifier - pipeline id:{pipeline_id} - New inserted Records Count is :',insert_df.count())

    update_df=updated_record_identifier(join_df,processed_table)
    print(f'updated_record_identifier - pipeline id:{pipeline_id} - Updated Records Count is :',update_df.count())

    no_change_df=no_change_record_identifier(join_df)
    print(f'no_change_record_identifier - pipeline id:{pipeline_id} - No Change Records Count is :',no_change_df.count())

    if globals()[index] > 0:
        parent_df=globals()[f"{processed_tables_list[globals()[index]-1]}_df"].alias("rightDf")
        insert_df=child_table_id_loader(insert_df.alias("leftDf"),parent_df,processed_table)

    union_df=insert_df.union(update_df).union(no_change_df).union(globals()[f"{processed_table}_tgt_history_df"])

    globals()[f"{processed_table}_df"]=union_df.cache()

    #write to a temp location.
    temp_location=processed_s3path+'tmp/'
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Writing Data to Temp location : {temp_location+processed_table}')
    write_data_to_processed(globals()[f"{processed_table}_df"],temp_location+processed_table)
    #reading from temp location
    temp_df=spark_reader(temp_location,processed_table)
    #write to a final location.
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Writing Data to Processed location : {processed_s3path+processed_table}')
    write_data_to_processed(temp_df,processed_s3path+processed_table)
    create_processed_table(pipeline_id,temp_df,processed_s3path+processed_table, app_name, domain_name,aws_region, env)
    print(f'historic_data_preservator - pipeline id:{pipeline_id} - Historic Data Preservation - Finished')

def new_insert_record_identifier(join_df,processed_table,max_seq_num):
    insert_record_df=(join_df
    .where(f"tgt_df.{key_columns[processed_table]} is null")
    .select("src_df.*")
    )
    print(f'sequence_generator - pipeline id:{pipeline_id} - Sequence generation - Started')
    insert_record_df=sequence_generator(insert_record_df,processed_table,max_seq_num)
    print(f'sequence_generator - pipeline id:{pipeline_id} - Sequence generation - Finished')
    return insert_record_df.cache()
    
def updated_record_identifier(join_df,processed_table):
    filter_df= join_df.where("src_md5<>tgt_md5")
    if globals()[index]>0:
        sequence_column=[sequence_columns[processed_table]] + [sequence_columns[processed_tables_list[globals()[index]-1]]]
    else:
        sequence_column=[sequence_columns[processed_table]]
    src_df_columns=filter_df.select("src_df.*").columns
    #marking end dates for records from target where there is a updated records from source
    end_date_update_df = (filter_df.select("tgt_df.*").withColumn("end_dt", when(col("start_dt") == today_dt,lit(date.today()).cast(DateType())).otherwise(date_sub(lit(date.today()),1).cast(DateType())))).select(*src_df_columns + ["start_dt", "end_dt"])
    #inserting new record for updated record from source.    
    update_df=(filter_df
    .select(*["src_df."+x for x in src_df_columns if x not in sequence_column]+["tgt_df."+x for x in sequence_column])
    .withColumn("start_dt",lit(date.today()).cast(DateType()))
    .withColumn("end_dt",lit("2999-12-31").cast(DateType()))).select(*src_df_columns+["start_dt","end_dt"])
    union_df=end_date_update_df.union(update_df)
    return union_df.cache()
    
def no_change_record_identifier(join_df):
    return (join_df
    .where("src_md5==tgt_md5")
    .select("tgt_df.*")
    ).cache()

def create_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name,aws_region, env):
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Started')
    athena_util.AthenaUtil.create_athena_processed_table(pipeline_id, dataframe, data_path, app_name, domain_name, aws_region, env)
    print(f'Athena Process Table Creation - pipeline id:{pipeline_id} - Creating Athena table - Finished')

def fee_rpt_app_phoenix_runner():
    """Runs all steps required for Phoenix Transform layer"""
    try:
        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Setting initial job status - Started')
        exec_id = job_status.put_jobstatus(inv_dict, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), env)
        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Setting initial job status - Finished')
        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Started')

        for domain_key,table_list_value in raw_tables_list.items():
            base_path_string=globals()[f'{domain_key}_base_s3path']
            final_table_reader(base_path_string,table_list_value)
        
        globals()[f"df_fee_sat"]=spark_reader(processed_s3path,"fee_sat")
        temp_view_creator(globals()[f"df_fee_sat"],"fee_sat")

        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Temporary View Creation On Landing Tables - Finished')
        for processed_table in processed_tables_list:
            globals()[index]=processed_tables_list.index(processed_table)
            print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Data Processing for Processed Table - {processed_table} - Started')
            folder_name=fee_rpt_prefix+processed_table+"/"
            if is_s3_path_empty(fee_rpt_finance_s3_bucket,folder_name):
                initial_data_processing(processed_s3path, processed_table)
            else:
                historic_data_preservator(processed_s3path, processed_table)

        job_end_time = datetime.now(timezone('US/Eastern'))
        print('job_start_time and job_end_time dynamodb entry: ', str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")),
              str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")))
        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Updating job status - Started')
        job_status.update_jobstatus(inv_dict, env, pipeline_id, exec_id, projectConstants.NONE, projectConstants.NONE, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.SUCCESS_FLAG, '', 0, 0, 0, sla_met)
        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'fee_rpt_app_phoenix_runner - pipeline id:{pipeline_id} - Completed')

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
    fee_rpt_app_phoenix_runner()