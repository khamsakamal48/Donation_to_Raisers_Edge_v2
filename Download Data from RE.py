import os
import logging
import base64
import msal
import datetime
import requests
import pandas as pd
import numpy as np
import glob
import json
import sys

from dotenv import load_dotenv
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from sqlalchemy import create_engine
from sqlalchemy import text


def set_current_directory():
    logging.info('Setting current directory')

    os.chdir(os.getcwd())


def start_logging():
    global process_name

    # Get File Name of existing script
    process_name = os.path.basename(__file__).replace('.py', '').replace(' ', '_')

    logging.basicConfig(filename=f'Logs/{process_name}.log', format='%(asctime)s %(message)s', filemode='w',
                        level=logging.DEBUG)

    # Printing the output to file for debugging
    logging.info('Starting the Script')


def stop_logging():
    logging.info('Stopping the Script')


def get_env_variables():
    logging.info('Setting Environment variables')

    global REMOTE_SERVER_IP, REMOTE_SERVER_USER, REMOTE_SERVER_PASS, O_CLIENT_ID, \
        CLIENT_SECRET, TENANT_ID, FROM, SEND_TO, CC_TO, ERROR_EMAILS_TO, DB_IP, DB_NAME, \
        DB_USERNAME, DB_PASSWORD, AUTH_CODE, REDIRECT_URL, CLIENT_ID, RE_API_KEY

    load_dotenv()

    REMOTE_SERVER_IP = os.getenv('REMOTE_SERVER_IP')
    REMOTE_SERVER_USER = os.getenv('REMOTE_SERVER_USER')
    REMOTE_SERVER_PASS = os.getenv('REMOTE_SERVER_PASS')
    O_CLIENT_ID = os.getenv('O_CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    TENANT_ID = os.getenv('TENANT_ID')
    FROM = os.getenv('FROM')
    SEND_TO = eval(os.getenv('SEND_TO'))
    CC_TO = eval(os.getenv('CC_TO'))
    ERROR_EMAILS_TO = eval(os.getenv('ERROR_EMAILS_TO'))
    DB_IP = os.getenv("DB_IP")
    DB_NAME = os.getenv("DB_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    AUTH_CODE = os.getenv("AUTH_CODE")
    REDIRECT_URL = os.getenv("REDIRECT_URL")
    CLIENT_ID = os.getenv("CLIENT_ID")
    RE_API_KEY = os.getenv("RE_API_KEY")


def send_error_emails(subject, Argument):
    logging.info('Sending email for an error')

    authority = f'https://login.microsoftonline.com/{TENANT_ID}'

    app = msal.ConfidentialClientApplication(
        client_id=O_CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=authority
    )

    scopes = ["https://graph.microsoft.com/.default"]

    result = None
    result = app.acquire_token_silent(scopes, account=None)

    if not result:
        result = app.acquire_token_for_client(scopes=scopes)

        TEMPLATE = """
        <table style="background-color: #ffffff; border-color: #ffffff; width: auto; margin-left: auto; margin-right: auto;">
        <tbody>
        <tr style="height: 127px;">
        <td style="background-color: #363636; width: 100%; text-align: center; vertical-align: middle; height: 127px;">&nbsp;
        <h1><span style="color: #ffffff;">&nbsp;Raiser's Edge Automation: {job_name} Failed</span>&nbsp;</h1>
        </td>
        </tr>
        <tr style="height: 18px;">
        <td style="height: 18px; background-color: #ffffff; border-color: #ffffff;">&nbsp;</td>
        </tr>
        <tr style="height: 18px;">
        <td style="width: 100%; height: 18px; background-color: #ffffff; border-color: #ffffff; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #455362;">This is to notify you that execution of Auto-updating Alumni records has failed.</span>&nbsp;</td>
        </tr>
        <tr style="height: 18px;">
        <td style="height: 18px; background-color: #ffffff; border-color: #ffffff;">&nbsp;</td>
        </tr>
        <tr style="height: 61px;">
        <td style="width: 100%; background-color: #2f2f2f; height: 61px; text-align: center; vertical-align: middle;">
        <h2><span style="color: #ffffff;">Job details:</span></h2>
        </td>
        </tr>
        <tr style="height: 52px;">
        <td style="height: 52px;">
        <table style="background-color: #2f2f2f; width: 100%; margin-left: auto; margin-right: auto; height: 42px;">
        <tbody>
        <tr>
        <td style="width: 50%; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #ffffff;">Job :</span>&nbsp;</td>
        <td style="background-color: #ff8e2d; width: 50%; text-align: center; vertical-align: middle;">&nbsp;{job_name}&nbsp;</td>
        </tr>
        <tr>
        <td style="width: 50%; text-align: center; vertical-align: middle;">&nbsp;<span style="color: #ffffff;">Failed on :</span>&nbsp;</td>
        <td style="background-color: #ff8e2d; width: 50%; text-align: center; vertical-align: middle;">&nbsp;{current_time}&nbsp;</td>
        </tr>
        </tbody>
        </table>
        </td>
        </tr>
        <tr style="height: 18px;">
        <td style="height: 18px; background-color: #ffffff;">&nbsp;</td>
        </tr>
        <tr style="height: 18px;">
        <td style="height: 18px; width: 100%; background-color: #ffffff; text-align: center; vertical-align: middle;">Below is the detailed error log,</td>
        </tr>
        <tr style="height: 217.34375px;">
        <td style="height: 217.34375px; background-color: #f8f9f9; width: 100%; text-align: left; vertical-align: middle;">{error_log_message}</td>
        </tr>
        </tbody>
        </table>
        """

        # Create a text/html message from a rendered template
        emailbody = TEMPLATE.format(
            job_name=subject,
            current_time=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            error_log_message=Argument
        )

        # Set up attachment data
        with open(f'Logs/{process_name}.log', 'rb') as f:
            attachment_content = f.read()
        attachment_content = base64.b64encode(attachment_content).decode('utf-8')

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': subject,
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': emailbody
                    },
                    'ToRecipients': get_recipients(ERROR_EMAILS_TO),
                    'Attachments': [
                        {
                            '@odata.type': '#microsoft.graph.fileAttachment',
                            'name': 'Process.log',
                            'contentBytes': attachment_content
                        }
                    ]
                },
                'SaveToSentItems': 'true'
            }

            requests.post(
                endpoint,
                headers={
                    'Authorization': 'Bearer ' + result['access_token']
                },
                json=email_msg
            )

        else:
            logging.info(result.get('error'))
            logging.info(result.get('error_description'))
            logging.info(result.get('correlation_id'))


def set_api_request_strategy():
    logging.info('Setting API Request strategy')

    global http

    # API Request strategy
    logging.info('Setting API Request Strategy')

    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['HEAD', 'GET', 'OPTIONS'],
        backoff_factor=10
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount('https://', adapter)
    http.mount('http://', adapter)


def get_recipients(email_list):
    value = []

    for email in email_list:
        email = {
            'emailAddress': {
                'address': email
            }
        }

        value.append(email)

    return value


def pagination_api_request(url, params):
    logging.info('Paginating API requests')

    # Housekeeping
    housekeeping()

    # Pagination request to retreive list
    while url:
        # Blackbaud API GET request
        re_api_response = get_request_re(url, params)

        # Incremental File name
        i = 1
        while os.path.exists(f'API_Response_RE_{process_name}_{i}.json'):
            i += 1

        with open(f'API_Response_RE_{process_name}_{i}.json', 'w') as list_output:
            json.dump(re_api_response, list_output, ensure_ascii=False, sort_keys=True, indent=4)

        # Check if a variable is present in file
        with open(f'API_Response_RE_{process_name}_{i}.json') as list_output_last:

            if 'next_link' in list_output_last.read():
                url = re_api_response['next_link']

            else:
                break


def retrieve_token():
    logging.info('Retrieve token for API connections')

    with open('access_token_output.json') as access_token_output:
        data = json.load(access_token_output)
        access_token = data['access_token']

    return access_token


def get_request_re(url, params):
    logging.info('Running GET Request from RE function')

    # Request Headers for Blackbaud API request
    headers = {
        # Request headers
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + retrieve_token(),
    }

    re_api_response = http.get(url, params=params, headers=headers).json()

    return re_api_response


def housekeeping():
    logging.info('Doing Housekeeping')

    # Housekeeping
    multiple_files = glob.glob('*_RE_*.json')

    # Iterate over the list of filepaths & remove each file.
    logging.info('Removing old JSON files')
    for each_file in multiple_files:
        try:
            os.remove(each_file)
        except:
            pass


def load_from_JSON_to_DB():
    logging.info('Loading from JSON to Database')

    # Get a list of all the file paths that ends with wildcard from in specified directory
    fileList = glob.glob('API_Response_RE_*.json')

    df = pd.DataFrame()

    for each_file in fileList:
        # Open Each JSON File
        with open(each_file, 'r') as json_file:
            # Load JSON File
            json_content = json.load(json_file)

            # Load from JSON to pandas
            reff = pd.json_normalize(json_content['value'])

            # Load to a dataframe
            df_ = pd.DataFrame(data=reff)

            # Append/Concat dataframes
            df = pd.concat([df, df_])

    return df


def connect_db():
    logging.info('Connecting to Database')

    # Create an engine instance
    alchemyEngine = create_engine(f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_IP}:5432/{DB_NAME}',
                                  pool_recycle=3600)

    # Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect()

    return dbConnection


def disconnect_db():
    logging.info('Disconnecting from Database')

    if db_conn:
        db_conn.close()


def get_emails():
    logging.info('Downloading emails from RE')
    url = 'https://api.sky.blackbaud.com/constituent/v1/emailaddresses?limit=5000'
    pagination_api_request(url, {})

    email_df = load_from_JSON_to_DB()
    email_df = email_df[['constituent_id', 'address']].copy()

    load_to_db(email_df, 'constituent_list')


def load_to_db(df, table):
    logging.info('Loading to Database')

    if table == 'campaign_list':
        # Truncate Table
        truncate_table(table)

    if table != 'campaign_list':
        # Renaming column name
        df.rename(columns={df.columns[-1]: 'details'}, inplace=True)

    # Loading to SQL DB
    df.to_sql(table, db_conn, if_exists='append', index=False)


def get_phones():
    logging.info('Downloading phones from RE')
    url = 'https://api.sky.blackbaud.com/constituent/v1/phones?limit=5000'
    pagination_api_request(url, {})

    phone_df = load_from_JSON_to_DB()
    phone_df = phone_df[['constituent_id', 'number']].copy()

    load_to_db(phone_df, 'constituent_list')


def truncate_table(table):
    logging.info('Truncating the table')

    db_conn.execute(text(f"TRUNCATE TABLE {table};"))
    db_conn.commit()

def get_campaign_list():
    logging.info('Downloading Campaign list from Raisers Edge')

    # Get Campaign List
    url = 'https://api.sky.blackbaud.com/nxt-data-integration/v1/re/campaigns?limit=5000'
    params = {}
    pagination_api_request(url, params)

    campaign_df = load_from_JSON_to_DB()
    campaign_df = campaign_df[['campaign_id', 'description', 'id']].copy()

    load_to_db(campaign_df, 'campaign_list')


try:
    # Start Logging for Debugging
    start_logging()

    # Set current directory
    set_current_directory()

    # Retrieve contents from .env file
    get_env_variables()

    # Housekeeping
    housekeeping()

    # Connect to DataBase
    db_conn = connect_db()

    # Truncate Table
    truncate_table('constituent_list')

    # Set API Request strategy
    set_api_request_strategy()

    # Get List of Alums with Email
    get_emails()

    # Get List of Alums with Phone
    get_phones()

    # Get Campaign List
    get_campaign_list()

except Exception as Argument:
    logging.error(Argument)

    send_error_emails('Error while downloading data | Donation to Raisers Edge', Argument)

finally:

    # Closing DB connections
    disconnect_db()

    # Housekeeping
    housekeeping()

    # Stop Logging
    stop_logging()

    sys.exit()
