import requests
import pandas as pd
import os
from datetime import date
from awsglue.utils import getResolvedOptions
import sys
import boto3
from ingestion_framework.constants import projectConstants

print("environment varialbe Initialization")
env = getResolvedOptions(sys.argv, ['JobEnvironment'])['JobEnvironment']  # "dev"
app_name = getResolvedOptions(sys.argv, ['AppName'])['AppName']  # "fee_rpt"
aws_region = getResolvedOptions(sys.argv, ['AWSRegion'])['AWSRegion']
domain_name = getResolvedOptions(sys.argv, ['DomainName'])['DomainName']  # "reference"
sts_client = boto3.client(projectConstants.STS)
aws_account_id = sts_client.get_caller_identity()[projectConstants.ACCOUNT]

fee_rpt_prefix = f"data/final/"
print(fee_rpt_prefix)
fee_rpt_s3_bucket = f"datamesh-{domain_name}-data-domain-{aws_account_id}-{env}-{aws_region}"
print(fee_rpt_s3_bucket)
final_base_s3path = f"s3://{fee_rpt_s3_bucket}/data/dropzone/cury_xcgh_rates/"
table_name = f"cury_xcgh_rates"
output_file = os.path.join(final_base_s3path, f"{table_name}.parquet")


def write_to_final(df, target_s3_path):
    df.to_parquet(target_s3_path)
    print("write complete")


def from_euro():
    listid = ['USD', 'GBP']
    appended_data = []
    for i in listid:
        # idlist = i
        url = f'https://sdw.ecb.europa.eu/quickview.do?SERIES_KEY=120.EXR.A.{i}.EUR.SP00.A'
        res = requests.get(url)
        df = pd.read_html(res.text)[5]
        del df["obs. status"]
        df.rename(columns={"Period ↓": "rpt_year", "value": "avg_year_xcgh_rate"}, inplace=True)
        df['cury_from_nm'] = 'EUR'
        df['cury_to_nm'] = i
        df = df.iloc[:, [0, 2, 3, 1]]
        print(df.info())
        df = df.iloc[3:]
        df['cury_from_nm'] = df['cury_from_nm'].astype("string")
        df['avg_year_xcgh_rate'] = df['avg_year_xcgh_rate'].astype('float64')
        df['cury_to_nm'] = df['cury_to_nm'].astype("string")
        df['rpt_year'] = df['rpt_year'].astype('int64')
        appended_data.append(df)
    appended_data = pd.concat(appended_data)
    return appended_data


def from_gbp():
    url = "https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?Travel=NIxIRxSUx&FromSeries=1&ToSeries=50&DAT=RNG&FD=1&FM=Jan&FY=2000&TD=31&TM=Dec&TY=2022&FNY=&CSVF=TT&html.x=184&html.y=9&C=DMY&Filter=N"
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}
    request = requests.get(url, headers=header)
    df = pd.read_html(request.text)[0]
    df.rename(columns={"Date": "rpt_year",
                       "Annual average Spot exchange rate, US $ into Sterling  XUAAUSS": "avg_year_xcgh_rate"},
              inplace=True)
    df['cury_from_nm'] = 'GBP'
    df['cury_to_nm'] = 'USD'
    df = df.iloc[:, [0, 2, 3, 1]]
    print(df.info())
    df.sort_values(by=['rpt_year'], inplace=True, ascending=False)
    df['rpt_year'] = pd.DatetimeIndex(df['rpt_year']).year
    df['cury_from_nm'] = df['cury_from_nm'].astype("string")
    df['cury_to_nm'] = df['cury_to_nm'].astype("string")
    df['rpt_year'] = df['rpt_year'].astype("int64")
    return df


def web_scrapper_runner():
    df1 = from_euro()
    df2 = from_gbp()
    final_df = pd.concat([df1, df2])
    final_df['load_dt'] = pd.to_datetime('today').date()
    print("final_data")
    print(final_df)
    write_to_final(final_df, output_file)


web_scrapper_runner()






