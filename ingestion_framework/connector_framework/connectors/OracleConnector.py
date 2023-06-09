# System Libraries
import decimal
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


class OracleConnector(abs_connector):

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
        self.gcd[projectConstants.PWD] = self.secret[projectConstants.PWD]
        self.gcd[projectConstants.HOST] = self.secret[projectConstants.HOST]
        self.gcd[projectConstants.PORT] = self.secret[projectConstants.PORT]
        self.gcd[projectConstants.DBNAME] = self.secret[projectConstants.DBNAME]

    #@logger_util.common_logger_exception
    def read_data(self):
        # Collect spark read options
        sparkReadOptions = self.get_spark_options(read=True)

        # Perform query given spark read options
        print(f"OracleConnector - Reading dataframe from {self.connector_config[dynamodbConstants.SOURCE_DATABASE_TYPE]} database...")
        try:
            return self.spark.read.options(**sparkReadOptions).format(projectConstants.JDBC).load()
        except Exception as err:
            print(f'Oralce Connector - Failed with error: {err}')
            print(f"Check Keys: {list(sparkReadOptions.keys())}")
            print(sys.exc_info())
            raise Exception(f'Oracle Connector - Failed with error: {err}')

    #@logger_util.common_logger_exception
    def write_data(self, df, write_location):
        # Collect spark write options
        sparkWriteOptions = self.get_spark_options(write=True)

        # Perform write to Oracle given spark write options and dataframe
        print(f"OracleConnector - Writing dataframe to {self.connector_config[dynamodbConstants.TARGET_TYPE]} database...")
        try:
            df.write.jdbc(**sparkWriteOptions)
        except Exception as err:
            print(f"Cannot write data to database: {self.connector_config[dynamodbConstants.TARGET_TYPE]}...")
            print(f"Check Keys: {list(sparkWriteOptions.keys())}")
            print(sys.exc_info())
            raise Exception(f'Oracle Connector - Failed with error: {err}')

    @logger_util.common_logger_exception
    def close_connection(self):
        pass

    #@logger_util.common_logger_exception
    def get_spark_options(self, read=False, write=False):
        source_table_name = self.connector_config[dynamodbConstants.SOURCE_TABLE_NAME]
        source_partition_col = self.connector_config.get(dynamodbConstants.SOURCE_PARTITION_COL)
        source_query = self.connector_config[dynamodbConstants.SOURCE_QUERY]
        
        oracle_host = self.gcd[projectConstants.HOST]
        oracle_port = self.gcd[projectConstants.PORT]
        oracle_dbName = self.gcd[projectConstants.DBNAME]
        sparkOptions = {
                projectConstants.URL: f"""jdbc:oracle:thin:@{oracle_host}:{oracle_port}:{oracle_dbName}""",
                projectConstants.USER: self.gcd[projectConstants.USERNAME],
                projectConstants.PWD: self.gcd[projectConstants.PWD],
                projectConstants.DRIVER: libConstants.oracleDriver,
            }
        if read:
            if source_partition_col is not None:
                sparkOptions[projectConstants.DBTABLE] = "(select min({}) as MIN_VAL, max({}) as MAX_VAL from {}) t".\
                    format(source_partition_col, source_partition_col, source_table_name)
                min_max = self.spark.read.options(**sparkOptions).format(projectConstants.JDBC).load().first().asDict()

                l_bound = min_max['MIN_VAL']
                u_bound = min_max['MAX_VAL']

                if type(min_max['MIN_VAL']) is decimal.Decimal:
                    l_bound = int(l_bound)
                    u_bound = int(u_bound)
                sparkOptions[projectConstants.PARTITION_COLUMN] = source_partition_col
                sparkOptions[projectConstants.LOWER_BOUND] = l_bound
                sparkOptions[projectConstants.UPPER_BOUND] = u_bound
                # Default value, but spark chooses the optimum 'numPartitions' number at runtime
                # based on min(lowerBound) and max(upperBound) values of read partition column
                sparkOptions[projectConstants.NUM_PARTITIONS] = 100
            sparkOptions[projectConstants.FETCH_SIZE] = projectConstants.DEFAULT_SPARK_JDBC_FETCH_SIZE,
            if source_query:
                sparkOptions[projectConstants.DBTABLE] = '(' + source_query + ') t'
            else:
                sparkOptions[projectConstants.DBTABLE] = source_table_name
        if write:
            target_table_name = self.connector_config[dynamodbConstants.TARGET_TABLE_NAME]
            sparkOptions[projectConstants.WRITE_MODE] = projectConstants.OVERWRITE_MODE
            sparkOptions[projectConstants.DBTABLE] = target_table_name

        return sparkOptions

    @logger_util.common_logger_exception
    def read_tgt_data(self):
        return None