# Author : Deepika S
# created : 07/06/2023
# modified :
# modified by:


"""This script is responsible for retrieving secret values from the secret manager and creating log files on a daily basis."""

import base64
import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


class GlobalConfig:
    """This function is to initialize variables"""

    def __init__(self):
        load_dotenv()
        self.secret_key = self.get_secrets()
        self.base_dir_for_all = '/home/ubuntu/Deployment/svn/REVAMP_Xandr/'
        self.current_date = (datetime.now()).date()
        file_name = self.base_dir_for_all + 'logs/global_' + str(self.current_date) + '.log'
        logging.basicConfig(filename=file_name,
                                filemode='a',
                                level=logging.DEBUG,
                                format='[%(asctime)s]: %(message)s',
                                datefmt='%m/%d/%Y %I:%M:%S %p')
        self.write_to_log("Logging file created")
        # Creating a session
        self.s3_client = boto3.resource('s3')
    def get_secrets(self):
        secret_name = os.getenv('SECRET')
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager', region_name=os.getenv('REGION'))
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        return json.loads(secret)

    @staticmethod
    def write_to_log(data, _type='info'):
        if _type == 'error':
            logging.error(data)
        else:
            logging.info(data)


