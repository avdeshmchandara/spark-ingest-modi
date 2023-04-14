'''
Module Name : datamesh_parser_raw.py
Called From Wrapper Module : parser_util.py
'''
import sys
from awsglue.utils import getResolvedOptions
from ingestion_framework.utils import parser_util
from ingestion_framework.constants import projectConstants

def main():
    # Get spark context
    if (f'--{projectConstants.JOB_ENVIRONMENT}' in sys.argv):
        job_env = getResolvedOptions(sys.argv, [projectConstants.JOB_ENVIRONMENT])[projectConstants.JOB_ENVIRONMENT]

    if (f'--{projectConstants.AWS_REGION}' in sys.argv):
        aws_region = getResolvedOptions(sys.argv, [projectConstants.AWS_REGION])[projectConstants.AWS_REGION]

    if not job_env:
        print("Job Environment is Mandatory Job Argument")
        raise SystemExit('Job Environment is Mandatory Job Argument')
        
    if not aws_region:
        print("Aws Region is Mandatory Job Argument")
        raise SystemExit('Aws Region is Mandatory Job Argument')

    parser_util.load_parser_util(job_env, aws_region)
    

if __name__ == "__main__":
    main()
