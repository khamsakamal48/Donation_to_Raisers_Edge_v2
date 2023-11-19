import os
import logging
import base64
import msal
import datetime
import requests
import sys
import json
import ast


from dotenv import load_dotenv
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib.parse import quote_plus


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
    DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
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

        template = """
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
        email_body = template.format(
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
                        'Content': email_body
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


def del_request_re(url):
    logging.info('Running DEL Request from RE function')

    # Request Headers for Blackbaud API request
    headers = {
        # Request headers
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + retrieve_token(),
    }

    http.delete(url, headers=headers)


def retrieve_token():
    logging.info('Retrieve token for API connections')

    with open('access_token_output.json') as access_token_output:
        data = json.load(access_token_output)
        access_token = data['access_token']

    return access_token


def stop_logging():
    logging.info('Stopping the Script')

try:

    # Start Logging for Debugging
    start_logging()

    # Set current directory
    set_current_directory()

    # Load Environment Variables
    get_env_variables()

    # Set API Request strategy
    set_api_request_strategy()

    # Get Gift IDs to delete
    gifts_to_delete = ast.literal_eval(input('Enter the Gift IDs to delete: ').replace(' ', ''))

    for each_gift in gifts_to_delete:
        del_request_re(f'https://api.sky.blackbaud.com/gift/v1/gifts/{each_gift}')

except Exception as Argument:

    logging.error(Argument)

    send_error_emails('Deleting Donation in Raisers Edge', Argument)

finally:

    stop_logging()

    sys.exit()
