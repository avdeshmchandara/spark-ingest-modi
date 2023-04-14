import sys
from awsglue.utils import getResolvedOptions
from ingestion_framework.scripts import job_initializer
from ingestion_framework.utils import deequ_util
from ingestion_framework.constants import projectConstants


def main():
    # Initializing spark and Logger
    
    if ('--{}'.format(projectConstants.APP_NAME) in sys.argv):
        app_name = getResolvedOptions(sys.argv, [projectConstants.APP_NAME])[projectConstants.APP_NAME]
    else:
        app_name = ""
        
    if ('--{}'.format(projectConstants.JOB_ENVIRONMENT) in sys.argv):
        job_env = getResolvedOptions(sys.argv, [projectConstants.JOB_ENVIRONMENT])[projectConstants.JOB_ENVIRONMENT]
    else:
        job_env = ""
        
    spark = job_initializer.initializeSpark(app_name)
    logger = job_initializer.getGlueLogger(spark)
    print('Deequ - Initialized Spark Session & logger') 
    failed_pipelines = []   
    try :
        failed_pipelines = deequ_util.deequ_validation(job_env,app_name)
    except Exception as error:
        print(f'Deequ - Failed with error: {error}')
        raise SystemExit(f'Deequ - Failed with error: {error}')
    
    if len(failed_pipelines) > 0:
        print(f"Failed pipelines are : {failed_pipelines}")
        raise SystemExit(f'Some Pipelines failed to run the DQ : {failed_pipelines}')

if __name__ == "__main__":
    main()