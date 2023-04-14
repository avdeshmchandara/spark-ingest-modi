import sys
from awsglue.utils import getResolvedOptions
from ingestion_framework.scripts import AppRunner
from ingestion_framework.constants import projectConstants


def main():
    # Reading the job parameters
    if ('--{}'.format(projectConstants.APP_NAME) in sys.argv):
        app_name = getResolvedOptions(sys.argv, [projectConstants.APP_NAME])[projectConstants.APP_NAME]
    else:
        app_name = ""
    
    if ('--{}'.format(projectConstants.PIPELINE_ID) in sys.argv):
        pipeline_id = getResolvedOptions(sys.argv, [projectConstants.PIPELINE_ID])[projectConstants.PIPELINE_ID]
    else:
        pipeline_id = ""
    
    if ('--{}'.format(projectConstants.JOB_ENVIRONMENT) in sys.argv):
        job_env = getResolvedOptions(sys.argv, [projectConstants.JOB_ENVIRONMENT])[projectConstants.JOB_ENVIRONMENT]
    else:
        job_env = ""
    
    if ('--{}'.format(projectConstants.RUN_ALL) in sys.argv):
        run_all_flag = getResolvedOptions(sys.argv, [projectConstants.RUN_ALL])[projectConstants.RUN_ALL]
    else:
        run_all_flag = ""
    
    if ('--{}'.format(projectConstants.VERSION) in sys.argv):
        version = getResolvedOptions(sys.argv, [projectConstants.VERSION])[projectConstants.VERSION]
    else:
        version = ""
    
    if ('--{}'.format(projectConstants.JOB_START_DATETIME) in sys.argv):
        job_start_datetime = getResolvedOptions(sys.argv, [projectConstants.JOB_START_DATETIME])[projectConstants.JOB_START_DATETIME]
    else:
        job_start_datetime = ""
    
    if ('--{}'.format(projectConstants.JOB_END_DATETIME) in sys.argv):
        job_end_datetime = getResolvedOptions(sys.argv, [projectConstants.JOB_END_DATETIME])[projectConstants.JOB_END_DATETIME]
    else:
        job_end_datetime = ""

    if ('--{}'.format(projectConstants.RUN_IN_DAILY_CHUNKS) in sys.argv):
        run_in_chunks_flag = getResolvedOptions(sys.argv, [projectConstants.RUN_IN_DAILY_CHUNKS])[projectConstants.RUN_IN_DAILY_CHUNKS]
    else:
        run_in_chunks_flag = ""

    if ('--{}'.format(projectConstants.SEQUENCE_RUN) in sys.argv):
        sequence_run = getResolvedOptions(sys.argv, [projectConstants.SEQUENCE_RUN])[projectConstants.SEQUENCE_RUN]
    else:
        sequence_run = ""

    if job_start_datetime == "" and job_end_datetime != "":
        print(f'AppRunner - Job end date time passed without a start date time')
        raise SystemExit(f'AppRunner - Job end date time passed without a start date time')

    AppRunner.run(app_name, run_all_flag, pipeline_id, version, job_env, job_start_datetime, job_end_datetime, run_in_chunks_flag,sequence_run)


if __name__ == "__main__":
    main()