from pytest_bdd import scenario, given, when, then, parsers
import boto3
import datetime
from datetime import date
from decimal import Decimal
from ingestion_framework.utils import logger_util

@logger_util.common_logger_exception
@scenario("test.feature", "Postgres Testing")
def test_arguments1():
    pass

@logger_util.common_logger_exception
@given(parsers.parse("Pipeline ID {InID}"), target_fixture="start_cucumbers1")
def start_cucumbers1(InID):
    global pipeline_id
    pipeline_id=InID
    global postgresreaderTested
    postgresreaderTested=False

@logger_util.common_logger_exception
@then(parsers.parse("Test Postgres Reader"), target_fixture="postgres_reader")
def postgres_reader():
    postgresreaderTested=True
    glue = boto3.client('glue',region_name='us-east-1')
    global postgresreaderJobRun
    postgresreaderJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy',Arguments = {'--PipelineID': pipeline_id,'--Flag':'R'})
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy', RunId=postgresreaderJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy', RunId=postgresreaderJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print ("postgres_reader_else****"+status['JobRun']['JobRunState'])
            break
    assert  status['JobRun']['JobRunState']=='SUCCEEDED'

@logger_util.common_logger_exception
@then(parsers.parse("Test Postgres Writer"), target_fixture="postgres_writer")
def postgres_writer():
    glue = boto3.client('glue',region_name='us-east-1') 
    myNewJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-writer-pytest',Arguments = {'--PipelineID': pipeline_id,'--Flag':'W'})
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-writer-pytest', RunId=myNewJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-writer-pytest', RunId=myNewJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print (status['JobRun']['JobRunState'])
            break
    assert  status['JobRun']['JobRunState']=='SUCCEEDED'

@logger_util.common_logger_exception
@scenario("test.feature", "Sybase Testing")
def test_arguments2():
    pass

@logger_util.common_logger_exception
@given(parsers.parse("Sybase InvID {InID}"), target_fixture="start_cucumbers2")
def start_cucumbers2(InID):
    global pipeline_id
    pipeline_id=InID
    global sybasereaderTested
    sybasereaderTested=False

@logger_util.common_logger_exception
@then(parsers.parse("Test Sybase Reader"), target_fixture="sybase_reader")
def sybase_reader():
    sybasereaderTested=True
    glue = boto3.client('glue',region_name='us-east-1')
    global sybasereaderJobRun
    sybasereaderJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-sybase-pytest',Arguments = {'--PipelineID': pipeline_id,'--Flag':'R'})
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-sybase-pytest', RunId=sybasereaderJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-sybase-pytest', RunId=sybasereaderJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print (status['JobRun']['JobRunState'])
            break
    assert  status['JobRun']['JobRunState']=='SUCCEEDED'

@logger_util.common_logger_exception
@then(parsers.parse("Test Sybase Writer"), target_fixture="sybase_writer")
def sybase_writer():
    glue = boto3.client('glue',region_name='us-east-1')
    myNewJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-sybase-pytest-writer',Arguments = {'--PipelineID': pipeline_id,'--Flag':'W'})
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-sybase-pytest-writer', RunId=myNewJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-sybase-pytest-writer', RunId=myNewJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print (status['JobRun']['JobRunState'])
            break
    assert  status['JobRun']['JobRunState']=='SUCCEEDED'

@logger_util.common_logger_exception
@scenario("test.feature", "Oracle Testing")
def test_arguments3():
    pass

@logger_util.common_logger_exception
@given(parsers.parse("Pipeline ID {InID}"), target_fixture="start_cucumbers3")
def start_cucumbers3(InID):
    global pipeline_id
    pipeline_id=InID

@logger_util.common_logger_exception
@then(parsers.parse("Test Oracle Reader"), target_fixture="oracle_reader")
def oracle_reader():
    glue = boto3.client('glue',region_name='us-east-1')
    global OracleJobRun
    OracleJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-oracle-pytest-copy',Arguments = {'--PipelineID': pipeline_id,'--Flag':'R'})
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-oracle-pytest-copy', RunId=OracleJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-oracle-pytest-copy', RunId=OracleJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print ("oracle_reader****"+status['JobRun']['JobRunState'])
            break
    assert  status['JobRun']['JobRunState']=='SUCCEEDED'

@logger_util.common_logger_exception
@then(parsers.parse("Test Oracle Writer"), target_fixture="oracle_writer")
def oracle_writer():
    glue = boto3.client('glue',region_name='us-east-1')
    ''' if postgresreaderTested:
    
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy', RunId=postgresreaderJobRun['JobRunId'])
        while True:
            status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy', RunId=postgresreaderJobRun['JobRunId'])
            if status['JobRun']['JobRunState']=='RUNNING':
                continue
            else:
                print ("postgres_writer_else_checkingold****"+status['JobRun']['JobRunState'])
                break
        
        myNewJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy',Arguments = {'--PipelineID': pipeline_id,'--Flag':'W'})
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy', RunId=myNewJobRun['JobRunId'])
        print ("postgres_writer_After_Start***"+status['JobRun']['JobRunState'])
        while True:
            status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-postgres-pytest-copy', RunId=myNewJobRun['JobRunId'])
            if status['JobRun']['JobRunState']=='RUNNING':
                continue
            else:
                print (status['JobRun']['JobRunState'])
                break
    else: ''' 
    myNewJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-oracle-pytest-writer',Arguments = {'--PipelineID': pipeline_id,'--Flag':'W'})
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-oracle-pytest-writer', RunId=myNewJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-oracle-pytest-writer', RunId=myNewJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print (status['JobRun']['JobRunState'])
            break
    assert  status['JobRun']['JobRunState']=='SUCCEEDED'


def main():
    print("Hello World!")
    glue = boto3.client('glue',region_name='us-east-1')
    myNewJobRun = glue.start_job_run(JobName='sbi-base-load-datamesh-test-sri-dev-pytest')
    status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-dev-pytest', RunId=myNewJobRun['JobRunId'])
    print (status['JobRun']['JobRunState'])
    while True:
        status=glue.get_job_run(JobName='sbi-base-load-datamesh-test-sri-dev-pytest', RunId=myNewJobRun['JobRunId'])
        if status['JobRun']['JobRunState']=='RUNNING':
            continue
        else:
            print (status['JobRun']['JobRunState'])
            break

    print('Completed')
'''if __name__ == "__main__":
    ct = datetime.datetime.now()
    ts = ct.timestamp()
    dq='01-25-2022 14:18:25'
    lastrunTimeStamp = datetime.datetime.strptime(dq,"%m-%d-%Y %H:%M:%S").timestamp()
    print(Decimal(lastrunTimeStamp))
    
    print(ts)'''