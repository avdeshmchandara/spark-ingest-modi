import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JobName", 'SrcDatabase','S3OutputPath', 'AWSRegion'])
script_processed="datamesh_metadata_load"
print(f'starting processing of {script_processed} - to load metadata information'} )
print(f'{script_processed} - Defining  spark context and glue context started ' )
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
print(f'{script_processed} - Defining  spark context and glue context finished ' )

print(f'{script_processed} - Initalizing the job started')
job = Job(glueContext)
job.init(args['JobName'], args)
print(f'{script_processed} - Initalizing the job finished')

s3_OUTPUT_PATH = args['S3OutputPath'] + '/'
print(f'{script_processed} - S3 output path - {s3_OUTPUT_PATH} ')


client = boto3.client('glue',region_name=args['AWSRegion'])
src_databaseName = args['SrcDatabase']

print(f'\n {script_processed} -  src_databaseName:  {src_databaseName}')

Tables = client.get_tables( DatabaseName = src_databaseName )
tableList = Tables ['TableList']
print('\n{script_processed} - tableList:   {tableList}')


print(f'{script_processed} - looping through the table list obtain table names ansd writing the metadata information into S3  bucket {s3_OUTPUT_PATH} through spark dataframe started')
try:
   for table in tableList:
      tableName = table['Name']
      print('\n tableName: ', tableName)
      
      datasource0 = glueContext.create_dynamic_frame.from_catalog(database = src_databaseName, table_name = tableName, transformation_ctx = "datasource0")
      
      resolvedData = datasource0.resolveChoice(
      specs=[('final_count', 'cast:String'), ('loading_count', 'cast:String'), ('landing_count', 'cast:String'),
             ('app_id', 'cast:String')])
             
      print(f'{script_processed} - writing the resolved_data dataframe into s3 location : {s3_OUTPUT_PATH}{tableName} started ')
      resolvedData.toDF().write.format("parquet").mode('overwrite').save(s3_OUTPUT_PATH + tableName + "/")
      print(f'{script_processed} - writing the resolved_data dataframe into s3 location : {s3_OUTPUT_PATH}{tableName} finished ')

   print(f'processing of {script_processed} - to load metadata information finished' )

except Exception as err:
   print(f'{script_processed}  - Failed with error: {err}')
   raise Exception(f'{script_processed}  - Failed with error: {err}')

job.commit()
