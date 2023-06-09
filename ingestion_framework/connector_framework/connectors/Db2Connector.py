# System Libraries
import sys
import boto3
import base64
import json
# User Libraries
from ingestion_framework.connector_framework.connectors.Connector import abs_connector
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.constants import libConstants
from ingestion_framework.utils import logger_util
from ingestion_framework.utils import common_util


class Db2Connector(abs_connector):
    @logger_util.common_logger_exception
    def __init__(self, spark, logger, env_type, connector_config):
        self.spark = spark
        self.logger = logger
        self.env_type = env_type
        self.connector_type = connector_config.connector_type
        self.connector_config = connector_config.resource_details

    @logger_util.common_logger_exception
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
        self.gcd[projectConstants.PWD] = self.secret[projectConstants.PWD]
        self.gcd[projectConstants.HOST] = self.secret[projectConstants.HOST]
        self.gcd[projectConstants.PORT] = self.secret[projectConstants.PORT]
        self.gcd[projectConstants.DBNAME] = self.secret[projectConstants.DBNAME]

    @logger_util.common_logger_exception
    def read_data(self):
        # Collect spark read options
        sparkReadOptions = self.get_spark_options(read=True)

        # Perform query given spark read options
        print(f"Db2Connector - Reading dataframe from {self.connector_config[dynamodbConstants.SOURCE_DATABASE_TYPE]} database...")
        try:
            return self.spark.read.options(**sparkReadOptions).format(projectConstants.JDBC).load()
        except:
            self.logger.error("Cannot read data from the source...")
            self.logger.error(f"Check Keys: {list(sparkReadOptions.keys())}")
            self.logger.error(sys.exc_info())

    @logger_util.common_logger_exception
    def write_data(self, df, write_location):
        # Collect spark write options
        sparkWriteOptions = self.get_spark_options(write=True)

        # Perform write to DB2 given spark write options and dataframe
        print(f"Db2Connector - Writing dataframe to {self.connector_config[dynamodbConstants.TARGET_TYPE]} database...")
        try:
            df.write.jdbc(**sparkWriteOptions)
        except:
            self.logger.error(f"Cannot write data to database: {self.connector_config[dynamodbConstants.TARGET_TYPE]}...")
            self.logger.error(f"Check Keys: {list(sparkWriteOptions.keys())}")
            self.logger.error(sys.exc_info())

    @logger_util.common_logger_exception
    def close_connection(self):
        pass

    @logger_util.common_logger_exception
    def get_spark_options(self, read=False, write=False):
        source_table_name = self.connector_config[dynamodbConstants.SOURCE_TABLE_NAME]
        source_query = self.connector_config[dynamodbConstants.SOURCE_QUERY]
        target_table_name = self.connector_config[dynamodbConstants.TARGET_TABLE_NAME]
        db2_host = self.gcd[projectConstants.HOST]
        db2_port = self.gcd[projectConstants.PORT]
        db2_dbName = self.gcd[projectConstants.DBNAME]
        sparkOptions = {
                projectConstants.URL: f"""jdbc:db2://{db2_host}:{db2_port}/{db2_dbName}""",
                projectConstants.USER: self.gcd[projectConstants.USERNAME],
                projectConstants.PWD: self.gcd[projectConstants.PWD],
                projectConstants.DRIVER: libConstants.db2Driver,
            }
        if read:
            sparkOptions[projectConstants.FETCH_SIZE] = projectConstants.DEFAULT_SPARK_JDBC_FETCH_SIZE,
            if source_query:
                sparkOptions[projectConstants.DBTABLE] = '(' + source_query + ') t'
            else:
                sparkOptions[projectConstants.DBTABLE] = source_table_name
        if write:
            sparkOptions[projectConstants.WRITE_MODE] = projectConstants.OVERWRITE_MODE
            sparkOptions[projectConstants.DBTABLE] = target_table_name
        return sparkOptions

    @logger_util.common_logger_exception
    def read_tgt_data(self):
        return None


