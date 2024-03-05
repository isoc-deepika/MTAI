# Author : Deepika
# created : 07/17/2023
# modified :
# modified by:


"""This script is to fetch all the data in the Xandr platform reports using APIs and push the data as raw, cleaned and curated file to S3
1.Pushing all the retrived data to raw path
2.Process the data then push it as cleaned and curated data
3.Insert the curated data to Redshift
"""
import time

import requests
import json
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from config import global_config
import psycopg2
import boto3
from io import StringIO
from datetime import datetime, timedelta

class Xandr:

    """This function is to initialize variables"""

    def __init__(self):
        self.global_config = global_config.GlobalConfig()
        self.secret_key = self.global_config.get_secrets()
        self.start_date = str(datetime.now().date() - timedelta(11))
        self.end_date = str(datetime.now().date() - timedelta(11))
        print('start_date:',self.start_date, '-','end_date:',self.end_date)
#        exit()
        self.s3_client = boto3.client('s3')
        self.file_names = self.secret_key['FILE_NAMES'].split(',')
        # self.final_fields = self.secret_key['FINAL_FIELDS'].split(',')
        self.redshift_con = psycopg2.connect(dbname=self.secret_key['REDSHIFT_DBNAME'],
                                             host=self.secret_key['REDSHIFT_HOST'],
                                             port=self.secret_key['REDSHIFT_PORT'],
                                             user=self.secret_key['REDSHIFT_USER'],
                                             password=self.secret_key['REDSHIFT_PASSWORD'])
        self.redshift_cur = self.redshift_con.cursor()

    """This method is to generate the access token"""

    def get_access_token(self):
        self.creds = {
            "auth": {
                "username": self.secret_key['USER_NAME'],
                "password": self.secret_key['PASSWORD']
            }
        }
        response = requests.post(self.secret_key['AUTH_URL'], json.dumps(self.creds))
        if response.status_code == 200:
            response = json.loads(response.content.decode('utf-8'))
            self.access_token = response['response']['token']
            self.headers = {
                "Authorization": "Bearer " + self.access_token,
                "Content-Type": "application/json"
            }
#            exit()

    """This method is to read the query config file from s3"""

    def read_query_config(self):
        try:
            self.global_config.write_to_log("Reading query file","Info")
            self.query_config = self.s3_client.get_object(Bucket=self.secret_key['BUCKET_NAME'], Key=self.secret_key['S3_QUERY_CONFIG_PATH'])['Body'].read()
            self.query_config = self.query_config.decode('utf-8')
            self.query_config = json.loads(self.query_config)
            self.query_file = self.query_config
            return self.query_file
        except Exception as e:
           self.global_config.write_to_log("Error while reading query_config")
           self.global_config.write_to_log(e)
           self.global_config.write_to_log(e.args)
           raise e

    def pull_data(self,field_report,type):
        df_report = pd.DataFrame()
#        df_report['day'] = None
        for type_ in field_report['Fields'][type]:
            data = {
                "report":
                    {
                        "report_type": field_report['report_type'],
                        "userType": field_report['userType'],
                        "columns": type_,
                        "start_date": str(self.start_date) + " 00:00:00",
                        "end_date": str(self.end_date) + " 23:59:59",
                        "timeGranularity": field_report['timeGranularity'],
                        "timezone": field_report['timezone'],
                        "format": field_report['format']
                    }
            }
            print("data:",data)
            loginLink = self.secret_key['LOGINLINK']
            client = requests.session()
            respLogin = client.post(loginLink, json=self.creds)
            print(respLogin)
            print(respLogin.content)
            data_report = client.post(loginLink, json=data, headers=self.headers)
            print(data_report)
            print(data_report.content)
#            exit()
            response_code = json.loads(data_report.content.decode('utf-8'))
#            print("response_code:",response_code)
            report_id = response_code['response']['report_id']
            download_url = self.secret_key['DOWNLOAD_URL'] + report_id
            report = requests.get(download_url, headers=self.headers)
            print(report)
            execution_status = json.loads(report.content.decode('utf-8'))['response']['execution_status']
            print(execution_status)
            while execution_status != 'ready':
                time.sleep(10)
                report = requests.get(download_url, headers=self.headers)
                execution_status = json.loads(report.content.decode('utf-8'))['response']['execution_status']
                print(execution_status)
            print(execution_status)
            download_report_url =  self.secret_key['DOWNLOAD_REPORT_URL']+ report_id
            data = requests.get(download_report_url, headers=self.headers)
            data = data.content.decode('utf-8').strip()
            df = pd.read_csv(StringIO(data), low_memory=False)
            print(df.shape)
            df_report = pd.concat([df_report,df],axis=1)
            df_report = df_report.loc[:, ~df_report.columns.duplicated()].copy()
        df_report['datasource'] = self.secret_key['DATASOURCE']
        df_report['datasource_id'] = self.secret_key['DATASOURCEID']
        df_report['datasource_client'] = field_report['datasource_client_name']
        df_report['datasource_client_id'] = field_report['datasource_client_id']
        df_report['report_name'] = field_report['report_name']
        df_report = df_report.loc[:, ~df_report.columns.duplicated()].copy()
        print(df_report.head())
        print(df_report.shape)
        return df_report


    """This function process the dataframe and push the csv file to S3 to raw, cleaned and curated folders """

    def process_data(self,df,key_value):
        try:
            datenum_dict = {}
            date_year_dict = {}
            date_month_dict = {}
            date_day_dict = {}
            date_dict = {}
            self.df_raw = df.copy()
            if 'day' in df.columns.tolist():
                for days in df['day'].unique():
                    if str(days) == 'nan':
                       continue
                    custom_date = datetime.strptime(str(days), '%Y-%m-%d')
                    datenum = custom_date.strftime('%Y%m%d')
                    datenum_dict[days] = datenum
                    date_year_dict[days] = custom_date.strftime('%Y')
                    date_month_dict[days] = custom_date.strftime('%m')
                    date_day_dict[days] = custom_date.strftime('%d')
                    date_dict[days] = custom_date.strftime('%d-%m-%Y')

                for key, value in datenum_dict.items():
                    df.loc[df['day'] == key, 'datenum'] = value
                for key, value in date_year_dict.items():
                    df.loc[df['day'] == key, 'Year'] = value
                for key, value in date_month_dict.items():
                    df.loc[df['day'] == key, 'Month'] = value
                for key, value in date_day_dict.items():
                    df.loc[df['day'] == key, 'Day'] = value

            if 'day' in df.columns.tolist():
                for date in df['day'].unique():
                    if str(date) == 'nan':
                        continue
                    query = "delete from {}.{} where date = '{}'".format(self.secret_key['REDSHIFT_SCHEMA'],
                                                                        self.secret_key['REDSHIFT_TABLE_NAME'].format(key_value),date)
                    print(query)
            self.redshift_cur.execute(query)
            self.redshift_con.commit()
            self.global_config.write_to_log("Successfully processed the df", "Info")
        except Exception as e:
            print(e)

    """This function is to convert the DataFrame to CSV file and move it to S3"""

    def convert_df_to_csv_and_push_to_S3(self, df, report_type, key_value):
        try:
            csv_buffer = StringIO()
            raw_csv_buffer = StringIO()

            csv_buffer.seek(0)
            raw_csv_buffer.seek(0)

            df.to_csv(csv_buffer, index=False, encoding='utf8')
            self.df_raw.to_csv(raw_csv_buffer, index=False, encoding='utf8')

            self.s3_client.put_object(Body=raw_csv_buffer.getvalue(),
                                    Bucket=self.secret_key['BUCKET_NAME'],
                                    Key=self.secret_key['S3_RAW_PATH'].format(report_type,key_value,self.start_date))
            self.s3_client.put_object(Body=csv_buffer.getvalue(),
                                    Bucket=self.secret_key['BUCKET_NAME'],
                                    Key=self.secret_key['S3_CLEANED_PATH'].format(report_type,key_value, self.start_date))
            self.s3_client.put_object(Body=csv_buffer.getvalue(),
                                    Bucket=self.secret_key['BUCKET_NAME'],
                                    Key=self.secret_key['S3_CURATED_PATH'].format(report_type,key_value))

#            self.global_config.write_to_log('Successfully pushed files to s3', 'Info')
            print('Successfully pushed csv files to S3')
            csv_buffer.close()
            raw_csv_buffer.close()
        except Exception as e:
            self.global_config.write_to_log('Error while pushing the data to s3', 'Error')
            self.global_config.write_to_log(e)
            self.global_config.write_to_log(e.args)
            print(e)

    """This function is to insert data to Redshift"""

    def insert_data_to_redshift(self,report_type, key_value):
        try:
            # copy to red shift table
            query = "copy {}.{} from 's3://{}/{}' iam_role '{}' IGNOREHEADER 1 " \
                    "csv;".format(self.secret_key['REDSHIFT_SCHEMA'],
                                  self.secret_key['REDSHIFT_TABLE_NAME'].format(key_value),
                                  self.secret_key['BUCKET_NAME'],
                                  self.secret_key['S3_CURATED_PATH'].format(report_type,key_value),
                                  self.secret_key['REDSHIFT_ROLE_ARN'])
            print(query)
            self.redshift_cur.execute(query)
            self.redshift_con.commit()
 #           self.global_config.write_to_log('Successfully inserted the data to Redshift', 'Info')
        except psycopg2.ProgrammingError as e:
 #           self.global_config.write_to_log('Error while pushing the data to Redshift', 'Error')
            print(e)
            self.redshift_con.rollback()
            self.redshift_cur.close()
            self.redshift_con.close()
            raise e

    """This function is to call all the other functions"""

    def start_process(self):
        self.global_config.write_to_log("Process started:", "Info")
        print("Process started:")
        self.read_query_config()
        self.get_access_token()
        for client_id in self.query_file:
            print(client_id)
            Field_details_ = self.query_file[client_id]
            for Field_details in Field_details_:
              if  Field_details['execute'] == "True":
                print('Field_details:',Field_details)
                for fields,key_value in zip(Field_details['Fields'],self.file_names):
                    df = self.pull_data(Field_details,fields)
                    self.process_data(df,key_value.format(Field_details['report_name']))
                    self.convert_df_to_csv_and_push_to_S3(df,Field_details['report_name'],key_value.format(Field_details['report_name']))
                    self.insert_data_to_redshift(Field_details['report_name'],key_value.format(Field_details['report_name']))


obj = Xandr()
obj.start_process()

