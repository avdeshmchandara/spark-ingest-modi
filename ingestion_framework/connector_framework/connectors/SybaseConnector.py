# System Libraries
import sys
import boto3
import base64
import json
import ast
# User Libraries
from ingestion_framework.connector_framework.connectors.Connector import abs_connector
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.constants import libConstants
from ingestion_framework.utils import logger_util
from ingestion_framework.utils import common_util


class SybaseConnector(abs_connector):

    @logger_util.common_logger_exception
    def __init__(self, spark, logger, env_type, connector_config):
        self.spark = spark
        self.logger = logger
        self.env_type = env_type
        self.connector_type = connector_config.connector_type
        self.connector_config = connector_config.resource_details

    #@logger_util.common_logger_exception
    def open_connection(self):
        if self.connector_type == projectConstants.SOURCE_TYPE :
            env_details = common_util.get_source_environment_details(self.connector_config, self.env_type)
        else :
            env_details = common_util.get_target_environment_details(self.connector_config, self.env_type)
        secret_manager_key = env_details[dynamodbConstants.DATABASE_SECRET_MANAGER_NAME]

        client = boto3.client(projectConstants.SEC_MGR)
        get_secret_value_response = client.get_secret_value(
                SecretId=secret_manager_key
            )
        if projectConstants.SEC_STR in get_secret_value_response:
            self.secret = json.loads(get_secret_value_response[projectConstants.SEC_STR])
        else:
            self.secret = json.loads(base64.b64decode(get_secret_value_response[projectConstants.SEC_BIN]))

        self.gcd = {}
        self.gcd[projectConstants.USERNAME] = self.secret[projectConstants.USERNAME]
        self.gcd[projectConstants.PWD]    = self.secret[projectConstants.PWD]
        self.gcd[projectConstants.HOST]   = self.secret[projectConstants.HOST]
        self.gcd[projectConstants.PORT]   = self.secret[projectConstants.PORT]
        self.gcd[projectConstants.DBNAME] = self.secret[projectConstants.DBNAME]

    #@logger_util.common_logger_exception
    def read_data(self):
        #Collect spark read options
        sparkReadOptions = self.get_spark_options(read=True)

        #Perform query given spark read options 
        print(f"SybaseConnector - Reading dataframe from {self.connector_config[dynamodbConstants.SOURCE_DATABASE_TYPE]} database...")
        try:
            return self.spark.read.options(**sparkReadOptions).format(projectConstants.JDBC).load()
        except Exception as err:
            print(f'Sybase Connector - Failed with error: {err}')
            print(f"Check Keys: {list(sparkReadOptions.keys())}")
            print(sys.exc_info())
            raise Exception(f'Sybase Connector - Failed with error: {err}')

    #@logger_util.common_logger_exception
    def write_data(self, df, write_location):
        #Collect spark write options
        sparkWriteOptions = self.get_spark_options(write=True)
        url = sparkWriteOptions[projectConstants.URL]
        del sparkWriteOptions[projectConstants.URL]

        target_table_name = self.connector_config[dynamodbConstants.TARGET_TABLE_NAME]

        #Perform write to Sybase given spark write options and dataframe
        print(f"SybaseConnector - Writing dataframe to {self.connector_config[dynamodbConstants.TARGET_TYPE]} database...")
        try:
            df.write.jdbc(url=url, table=target_table_name, mode=projectConstants.OVERWRITE_MODE, properties=sparkWriteOptions)
        except Exception as err:
            print(f"Cannot write data to database: {self.connector_config[dynamodbConstants.TARGET_TYPE]}...")
            print(f"Check Keys: {list(sparkWriteOptions.keys())}")
            print(sys.exc_info())
            raise Exception(f'Sybase Connector - Failed with error: {err}')

    @logger_util.common_logger_exception
    def close_connection(self):
        pass

    #@logger_util.common_logger_exception
    def get_spark_options(self, read=False, write=False):
        source_query = self.connector_config[dynamodbConstants.SOURCE_QUERY]
        target_table_name = self.connector_config[dynamodbConstants.TARGET_TABLE_NAME]
        sybase_host = self.gcd[projectConstants.HOST]
        sybase_port = self.gcd[projectConstants.PORT]
        sybase_dbName = self.gcd[projectConstants.DBNAME]
        sparkOptions = {
                projectConstants.URL: f"""jdbc:sybase:Tds:{sybase_host}:{sybase_port}/{sybase_dbName}?""",
                projectConstants.USER: self.gcd[projectConstants.USERNAME],
                projectConstants.PWD: self.gcd[projectConstants.PWD],
                projectConstants.DRIVER: libConstants.sybaseDriver,
            }
        if read:
            sparkOptions[projectConstants.FETCH_SIZE] = projectConstants.DEFAULT_SPARK_JDBC_FETCH_SIZE,
            sparkOptions[projectConstants.QUERY] = source_query
        if write:
            sparkOptions[projectConstants.WRITE_MODE] = projectConstants.OVERWRITE_MODE
            sparkOptions[projectConstants.DBTABLE] = target_table_name

        return sparkOptions



    @logger_util.common_logger_exception
    def read_tgt_data(self):
        return None

        

    