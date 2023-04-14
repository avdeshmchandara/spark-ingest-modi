'''
Type : AWS Glue Script
Description : Called from Wrapper module - dynamodb_util.py
'''
import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from ingestion_framework.scripts import job_initializer
from ingestion_framework.utils import dynamodb_util
from ingestion_framework.constants import dynamodbConstants


def main():
    # Get spark context
    spark = job_initializer.initializeSpark(dynamodbConstants.DYNAMODB_TBL_CLEANUP)

    table_name = ''
    if ('--{}'.format(dynamodbConstants.DYNAMODB_TABLE_NAME) in sys.argv):
        table_name = getResolvedOptions(sys.argv, [dynamodbConstants.DYNAMODB_TABLE_NAME])[dynamodbConstants.DYNAMODB_TABLE_NAME]

    sort_key = ''
    if ('--{}'.format(dynamodbConstants.SORT_KEY) in sys.argv):
        sort_key = getResolvedOptions(sys.argv, [dynamodbConstants.SORT_KEY])[dynamodbConstants.SORT_KEY]
  
    partition_key = ''
    if ('--{}'.format(dynamodbConstants.PARTITION_KEY) in sys.argv):
        partition_key = getResolvedOptions(sys.argv, [dynamodbConstants.PARTITION_KEY])[dynamodbConstants.PARTITION_KEY]
  
    sk_values = ''
    if ('--{}'.format(dynamodbConstants.SK_VALUES) in sys.argv):
        sk_values = getResolvedOptions(sys.argv, [dynamodbConstants.SK_VALUES])[dynamodbConstants.SK_VALUES]

    logger = logging.getLogger(dynamodbConstants.DYNAMODB_TBL_CLEANUP)

    dynamodb_util.JobStatus(spark, logger).bulk_delete_dynamodb(table_name, sort_key, partition_key, sk_values)


if __name__ == "__main__":
    main()
