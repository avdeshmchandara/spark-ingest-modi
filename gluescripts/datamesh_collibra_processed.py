'''
Module Name : datamesh_collibra_processed.py
Called From Wrapper Module : collibra_util.py
'''
import sys
from awsglue.utils import getResolvedOptions
from ingestion_framework.utils import collibra_util
from ingestion_framework.constants import projectConstants


def main():
    ''' Wrapper Script Start.'''
    # Reading the job parameters

    if (f'--{projectConstants.JOB_ENVIRONMENT}' in sys.argv):
        job_env = getResolvedOptions(sys.argv, [projectConstants.JOB_ENVIRONMENT])[projectConstants.JOB_ENVIRONMENT]

    if (f'--{projectConstants.AWS_REGION}' in sys.argv):
        aws_region = getResolvedOptions(sys.argv, [projectConstants.AWS_REGION])[projectConstants.AWS_REGION]

    if (f'--{projectConstants.COLLIBRA_TOKEN}' in sys.argv):
        collibra_token = getResolvedOptions(sys.argv, [projectConstants.COLLIBRA_TOKEN])[
            projectConstants.COLLIBRA_TOKEN]

    if (f'--{projectConstants.JOB_MONITORING_URL}' in sys.argv):
        job_monitoring_url = getResolvedOptions(sys.argv, [projectConstants.JOB_MONITORING_URL])[
            projectConstants.JOB_MONITORING_URL]

    if (f'--{projectConstants.COLLIBRA_URL}' in sys.argv):
        collibra_url = getResolvedOptions(sys.argv, [projectConstants.COLLIBRA_URL])[projectConstants.COLLIBRA_URL]

    if not job_env:
        print("Job Environment is Mandatory Job Argument")
        raise SystemExit('Job Environment is Mandatory Job Argument')
    if not aws_region:
        print("Aws Region is Mandatory Job Argument")
        raise SystemExit('Aws Region is Mandatory Job Argument')
    if not collibra_token:
        print("Collibra Token is Mandatory Job Argument")
        raise SystemExit('Collibra Token is Mandatory Job Argument')
    if not job_monitoring_url:
        print("collibra jobs is Mandatory Job Argument")
        raise SystemExit('collibra jobs is Mandatory Job Argument')
    if not collibra_url:
        print("collibra_url is Mandatory Job Argument")
        raise SystemExit('collibra_url is Mandatory Job Argument')

    collibra_util.load_collibra_processed(job_env, aws_region, collibra_token, job_monitoring_url, collibra_url)


if __name__ == "__main__":
    main()
