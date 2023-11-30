import os
import paramiko
import logging
import base64
import msal
import datetime
import requests


from dotenv import load_dotenv
from datetime import datetime


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

    global REMOTE_SERVER_IP, REMOTE_SERVER_USER, REMOTE_SERVER_PASS, O_CLIENT_ID, CLIENT_SECRET, TENANT_ID, FROM, SEND_TO, CC_TO, ERROR_EMAILS_TO

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


def connect_server():
    logging.info(f'Creating SSH connection with {REMOTE_SERVER_IP}')

    global ssh, sftp

    # CONNECT TO SFTP
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=REMOTE_SERVER_IP, username=REMOTE_SERVER_USER, password=REMOTE_SERVER_PASS)
    sftp = ssh.open_sftp()


def download_files(remote_path, local_path):
    logging.info(f'Downloading files from {remote_path}')

    # DOWNLOAD ALL FILES IN FOLDER
    files = sftp.listdir(remote_path)
    for file in files:
        sftp.get(os.path.join(remote_path, file), os.path.join(local_path, file))

        # Removing files
        logging.info(f'Removing files from {remote_path}')
        sftp.remove(os.path.join(remote_path, file))


def close_connection():
    logging.info('Closing SSH & SFTP Connections')

    # CLOSE CONNECTION
    sftp.close()
    ssh.close()


try:

    # Start Logging for Debugging
    start_logging()

    # Set current directory
    set_current_directory()

    # Retrieve contents from .env file
    get_env_variables()

    # Connect to Server
    connect_server()

    # Download Donation files
    download_files('/F:/FTPServer/Kamal/Donation/', 'Files/Donation')

    # Download Receipts
    download_files('/F:/FTPServer/Kamal/Receipt/', 'Files/Receipts')

    # Download Thank You Letters
    download_files('/F:/FTPServer/Kamal/ThankyouLetter/', 'Files/Thank You Letters')


except Exception as Argument:

    logging.error(Argument)

    send_error_emails('Downloading Donation Files from Server', Argument)

finally:

    # Close connections
    close_connection()

    # Stop Logging
    stop_logging()

    exit()
