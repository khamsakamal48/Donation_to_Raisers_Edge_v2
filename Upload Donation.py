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
import pickle
import unicodedata
import re
import random
import string
import sys
import urllib.parse

from dotenv import load_dotenv
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from nameparser import HumanName
from tensorflow import keras
from sklearn.metrics import f1_score
from sqlalchemy import text
from fuzzywuzzy import process
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


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


def load_donation():
    logging.info('Loading donation CSV files to a DataFrame')

    all_files = glob.glob(os.path.join('Files/Donation', "*.csv"))

    # Checking if there are any files to upload
    if all_files:
        df_from_each_file = (pd.read_csv(file) for file in all_files)
        concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
        concatenated_df = concatenated_df.drop_duplicates().reset_index(drop=True).copy()
    else:
        logging.info('No donation CSV files to upload')
        concatenated_df = pd.DataFrame()

    return concatenated_df


def housekeeping():
    logging.info('Doing Housekeeping')

    os.chdir('Files/Donation')

    # Housekeeping
    multiple_files = glob.glob('*.csv')

    # Iterate over the list of filepaths & remove each file.
    logging.info('Removing old Donation CSV files')
    for each_file in multiple_files:
        try:
            os.remove(each_file)
        except:
            pass

    os.chdir('../../')


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

    try:
        db_conn.close()

    except:
        logging.error('Database not connected')
        pass


def get_from_db(table):
    logging.info('Fetching from Database')

    db = pd.read_sql_query(f'SELECT * FROM {table};', db_conn)

    return db


def get_missing_donations():
    logging.info('Identifying pending donations')

    df = donation_data[
        ~(donation_data['dtlDonor_id'].astype(int).isin(uploaded['dtldonor_id'].astype(int)))
    ].reset_index(drop=True)

    return df


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


def locate_donor(df):
    email = df['email']
    email = np.NaN if email.lower() in ['dean.acr.office@iitb.ac.in', 'dean.acr.office@gmail.com', 'donationrecipts@gmail.com', 'donationreceipts@gmail.com'] else email

    phone = df['contactno']
    pan = df['pancard']

    if len(phone) > 3:
        const_id = pd.read_sql_query(
            f'''
            SELECT DISTINCT id FROM valid_constituents
            WHERE id IN (
                SELECT DISTINCT constituent_id FROM constituent_list
                WHERE LOWER(details) IN (LOWER('{email}'), '{phone}')
                );
            ''',
            db_conn
        )

    else:
        const_id = pd.read_sql_query(
            f'''
            SELECT DISTINCT id FROM valid_constituents
            WHERE id IN (
                SELECT DISTINCT constituent_id FROM constituent_list
                WHERE LOWER(details) IN (LOWER('{email}'))
                );
            ''',
            db_conn
        )

    # Case-Match statement to get RE ID
    match const_id.shape[0]:

        # Found only one match
        case 1:
            return const_id.loc[0, 'id']

        # Found no match
        case 0:
            logging.info('Found no matches')
            const_id = search_constituent(email, phone, pan)

            match len(const_id):
                case 1:
                    return list(const_id)[0]

                case 0:
                    logging.info('Found no match')

                    # Create constituent
                    return create_constituent(df)

                case _:
                    logging.info('Found multiple matches')
                    return found_multiple_matches(df)

        case _:
            logging.info('Found multiple matches')
            return found_multiple_matches(df)


def found_multiple_matches(df):
    logging.info('Informing DB Administrator that multiple records were identified')

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

        template = '''
        <p style="text-align: justify;">Hi,</p>
        <p style="text-align: justify;">This is to inform you that for the below donor, I could find multiple matches in Raisers Edge. Hence, it could be that these are duplicate records, or else that either the same email, phone number, or PAN card number exists across one or more records.</p>
        <table style="width: 100%; border-collapse: collapse; border-style: hidden; height: 36px;" border="1">
        <tbody>
        <tr style="height: 18px;">
        <th style="width: 100%; height: 18px; background-color: #0099ff; text-align: center; vertical-align: middle;">
        <h2><span style="color: #ffffff;">Donor Details</span></h2>
        </th>
        </tr>
        <tr style="height: 18px;">
        <td style="width: 100%; height: 18px; border-style: hidden; background-color: #e6f5ff; text-align: center; vertical-align: middle;">
        <h4><span style="color: #333300;">{df}</span></h4>
        </td>
        </tr>
        </tbody>
        </table>
        <p>&nbsp;</p>
        <p>Thanks and Regards,</p>
        <p>A Bot.</p>
        '''

        # Create a text/html message from a rendered template
        email_body = template.format(
            df=pd.DataFrame(df).fillna('').T.to_html(index=False)
        )

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': 'Found multiple matches for Donors in Raisers Edge',
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': email_body
                    },
                    'ToRecipients': get_recipients(SEND_TO),
                    'CcRecipients': get_recipients(CC_TO),
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

        return None


def create_constituent(df):
    logging.info('Creating a new constituent')

    # Verify whether the donor is Individual or Organisation
    affiliation = df['affilation']
    company_name = df['companyname']
    name = df['name']

    if pd.isnull(company_name) or affiliation != 'Foundation' or affiliation != 'Corporate Non CSR':
        # Individual
        name = name.replace('\r\n', ' ').replace('\t', ' ').replace('\n', ' ').replace('  ', ' ')

        # Get First, Middle and Last Name
        name = HumanName(str(name))
        first_name = str(name.first).title()

        # In case there's no middle name
        try:
            middle_name = str(name.middle).title()
        except:
            middle_name = ''

        last_name = str(name.last).title()
        if not last_name:
            last_name = '.'

        # Get Gender
        gender = get_gender(first_name)

        title = str(name.title).title()
        if not title:
            match gender:
                case 'Male':
                    title = 'Mr.'
                case 'Female':
                    title = 'Ms.'

        # Parameters
        params = {
            'type': 'Individual',
            'first': first_name,
            'middle': middle_name,
            'last': last_name,
            'title': title,
            'gender': gender,
            'marital_status': 'Single',
            'primary_addressee': {
                'configuration_id': 7
            },
            'primary_salutation': {
                'custom_format': False,
                'configuration_id': 1
            }
        }

        constituent_code = 'Well Wisher'

    else:
        #Organization
        params = {
            'type': 'Organization',
            'name': name.replace('\r\n', ' ').replace('\t', ' ').replace('\n', ' ').replace('  ', ' ').strip()[:60]
        }

        constituent_code = 'Trust/Foundation' if affiliation == 'Foundation' else 'Business/Corporation'

    url = 'https://api.sky.blackbaud.com/constituent/v1/constituents'

    response = post_request_re(url, params)

    const_id = response['id']

    # Add constituent code
    url = 'https://api.sky.blackbaud.com/constituent/v1/constituentcodes'
    params = {
        'constituent_id': const_id,
        'description': constituent_code
    }

    post_request_re(url, params)

    # Inform that a new donor was created
    inform_abt_new_record(const_id, df)

    return const_id


def inform_abt_new_record(const_id, df):
    logging.info('Informing DB Administrator that a new record is created')

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

        template = '''
        <p style="text-align: justify;">Hi,</p>
        <p style="text-align: justify;">This is to inform you that a new record was created in Raisers Edge to add the donations for which we couldn't find any existing donors.</p>
        <p style="text-align: justify;">I request that you kindly review the record whether it's created correctly or check whether there was already a record of the same donor which I couldn't identify.</p>
        <p style="text-align: justify;">&nbsp;</p>
        <p style="text-align: justify;">Below are its details:</p>
        <table style="height: 126px; width: 100%; border-collapse: collapse; margin-left: auto; margin-right: auto;" border="1">
        <tbody>
        <tr style="height: 54px;">
        <th style="width: 100%; border-style: double; height: 54px; background-color: #ff9966; text-align: center; vertical-align: middle;">
        <h2><a href="https://host.nxt.blackbaud.com/constituent/records/{const_id}?envId=p-dzY8gGigKUidokeljxaQiA&svcId=renxt" target="_blank"><span style="color: #ffffff;">Open in RE</span></a></h2>
        </th>
        </tr>
        <tr style="height: 18px;">
        <td style="width: 100%; height: 18px;">&nbsp;</td>
        </tr>
        <tr style="height: 18px;">
        <td style="width: 100%; height: 18px; border-style: inset; background-color: #00cc99; text-align: center; vertical-align: middle;">
        <h2><span style="color: #ffffff;">Donor Details</span></h2>
        </td>
        </tr>
        <tr style="height: 18px;">
        <td style="width: 100%; height: 18px; background-color: #f0f0f5; text-align: center; vertical-align: middle;">{df}</td>
        </tr>
        </tbody>
        </table>
        <p style="text-align: justify;">&nbsp;</p>
        <p style="text-align: justify;">Thanks &amp; Regards,</p>
        <p style="text-align: justify;">A Bot.</p>
        '''

        # Create a text/html message from a rendered template
        email_body = template.format(
            const_id=const_id,
            df=pd.DataFrame(df).fillna('').T.to_html(index=False)
        )

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': 'Request to review new record/donor created in Raisers Edge',
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': email_body
                    },
                    'ToRecipients': get_recipients(SEND_TO),
                    'CcRecipients': get_recipients(CC_TO),
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


def post_request_re(url, params):
    logging.info('Running POST Request to RE function')

    # Request headers
    headers = {
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + retrieve_token(),
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache'
    }

    # Convert int64 to int in params
    params = {k: int(v) if isinstance(v, np.int64) else v for k, v in params.items()}

    try:
        if '&' in str(params):
            # Quote_plus for encoding special characters in URL
            encoded_params = {k: quote_plus(str(v)) for k, v in params.items()}
            re_api_response = http.post(url, headers=headers, json=encoded_params)
        else:
            # Convert int64 to int in params
            re_api_response = http.post(url, params=params, headers=headers, json=params)

        re_api_response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        raise Exception("The POST request was not successful.")

    except requests.exceptions.RequestException as err:
        logging.error(f"Request error occurred: {err}")
        raise Exception("An error occurred during the POST request.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise Exception

    else:
        return re_api_response.json()


def get_gender(name):
    loaded_model = load_model()

    try:
        name = name.lower()
    except:
        name = name.casefold()

    # Normalize the name to remove diacritic marks and special characters
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')

    name = prepare_encod_names([name])  # Now the names are encod as a vector of numbers with weight
    resu = (loaded_model.predict(name) > 0.5).astype("int32")

    if int(resu[0][0]) == 1:
        return 'Male'
    else:
        return 'Female'


def set_flag(i):
    # Builds an empty line with a 1 at the index of character
    vocab = {' ',
             'END',
             'a',
             'b',
             'c',
             'd',
             'e',
             'f',
             'g',
             'h',
             'i',
             'j',
             'k',
             'l',
             'm',
             'n',
             'o',
             'p',
             'q',
             'r',
             's',
             't',
             'u',
             'v',
             'w',
             'x',
             'y',
             'z'}
    len_vocab = len(vocab)

    aux = np.zeros(len_vocab)
    aux[i] = 1
    return list(aux)


# Truncate names and create the matrix
def prepare_encod_names(X):
    char_index = load_index()
    maxlen = 20

    vec_names = []
    trunc_name = [str(i)[0:maxlen] for i in X]  # consider only the first 20 characters
    for i in trunc_name:
        tmp = [set_flag(char_index.get(j, char_index[" "])) for j in str(i)]
        for k in range(0, maxlen - len(str(i))):
            tmp.append(set_flag(char_index["END"]))
        vec_names.append(tmp)
    return vec_names


# Load the char_index dictionary
def load_index():
    char_index_path = 'Models/char_index.pickle'
    with open(char_index_path, 'rb') as file:
        return pickle.load(file)


def load_model():
    # Load the trained model
    model_path = 'Models/model.h5'
    custom_objects = {'f1_score': f1_score}
    loaded_model = keras.models.load_model(model_path, custom_objects=custom_objects)
    return loaded_model


# Load the tokenizer
def load_token():
    tokenizer_path = 'Models/tokenizer.pickle'
    with open(tokenizer_path, 'rb') as file:
        return pickle.load(file)


def search_constituent(email, phone, pan):
    i = 0
    params = {}

    # Formatting phone
    phone = ''.join([x for x in phone if x.isdigit()])
    phone = phone if len(phone) > 6 else np.NaN

    for search_text in [email, pan, phone]:

        if not pd.isnull(search_text):

            if i == 0:
                # Check on the basis of email address
                url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/search?search_text={search_text}&search_field=email_address'

            else:
                url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/search?search_text={search_text}'

            response = get_request_re(url, params)

            if response['count'] != 0:
                return set(int(x['id']) for x in response['value'][:])

        i += 1

    return {}


def upload_donation(df, const_id):
    logging.info(f"Uploading donation for RE ID: {const_id} and Donation Portal Ref. No.: {df['dtlDonor_id']}")

    # Search Campaign
    camp_id = get_campaign(df['project'])

    # Identify Gift and Receipt Date
    date_1 = pd.to_datetime(df['transdate'])
    date_2 = pd.to_datetime(df['depositeddate'])

    if date_1 < date_2:
        gift_date = date_1.isoformat()
        receipt_date = date_2.isoformat()
    else:
        gift_date = date_2.isoformat()
        receipt_date = date_1.isoformat()

    # Get foreign currency type
    match df['currency']:

        case 'USD':
            f_currency_type = 'Amount in US Dollars'

        case 'SGD':
            f_currency_type = 'Amount in SG Dollars'

        case 'GBP':
            f_currency_type = 'Amount in Pounds'

        case 'CAD':
            f_currency_type = 'Amount in CAD Dollars'

        case _:
            f_currency_type = ''

    # Gift Parameters
    params = {
        'acknowledgements': [
            {
                'date': receipt_date,
                'status': 'ACKNOWLEDGED',
                'letter': 'General Thank You'
            }
        ],
        'amount': {
            'value': df['donationamount']
        },
        'constituent_id': const_id,
        'date': gift_date,
        'gift_splits': [{
            'amount': {
                'value': df['donationamount']
            },
            'campaign_id': int(camp_id),
            'fund_id': 457 if df['office'] == 'HF' else 458
        }],
        'type': 'Donation',
        'payments': [{
            'check_date': {
                'd': pd.to_datetime(gift_date).strftime('%d') if df['provid'] == 'Cheque' else '',
                'm': pd.to_datetime(gift_date).strftime('%m') if df['provid'] == 'Cheque' else '',
                'y': pd.to_datetime(gift_date).strftime('%Y') if df['provid'] == 'Cheque' else ''
            },
            'check_number': '' if pd.isnull(df['chequeno']) else df['chequeno'],
            'payment_method': 'PersonalCheck' if df['provid'] == 'Cheque' else 'Other',
            'reference': '' if df['provid'] == 'Cheque' else df['provid'],
            'reference_date': {
                'd': pd.to_datetime(gift_date).strftime('%d'),
                'm': pd.to_datetime(gift_date).strftime('%m'),
                'y': pd.to_datetime(gift_date).strftime('%Y')
            }
        }],
        'receipts': [{
            'amount': {
                'value': 0 if df['totalamount'] < 0 else df['totalamount']
            },
            'date': receipt_date,
            'status': 'RECEIPTED'
        }],
        'custom_fields': [
            {
                'category': '' if pd.isnull(df['affilation']) else 'Affiliation',
                'value': '' if pd.isnull(df['affilation']) else df['affilation'],
                'date': '' if pd.isnull(df['affilation']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['csrtype']) else 'CSR Type',
                'value': '' if pd.isnull(df['csrtype']) else df['csrtype'],
                'date': '' if pd.isnull(df['csrtype']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['dtlDonor_id']) else 'Donation Portal Reference No.',
                'value': '' if pd.isnull(df['dtlDonor_id']) else df['dtlDonor_id'],
                'date': '' if pd.isnull(df['dtlDonor_id']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['gifttype']) else 'Gift Type',
                'value': '' if pd.isnull(df['gifttype']) else df['gifttype'],
                'date': '' if pd.isnull(df['gifttype']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['provid']) else 'Payment Portal Method',
                'value': '' if pd.isnull(df['provid']) else df['provid'],
                'date': '' if pd.isnull(df['provid']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['paymenttype']) else 'Payment Type',
                'value': '' if pd.isnull(df['paymenttype']) else df['paymenttype'],
                'date': '' if pd.isnull(df['paymenttype']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': f_currency_type,
                'value': '' if f_currency_type == '' else df['currencyamount'],
                'date': '' if f_currency_type == '' else receipt_date
            },
            {
                'category': '' if pd.isnull(df['hfgrant']) else 'Grant No.',
                'value': '' if pd.isnull(df['hfgrant']) else df['hfgrant'],
                'date': '' if pd.isnull(df['hfgrant']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['transid']) else 'Transaction ID',
                'value': '' if pd.isnull(df['transid']) else df['transid'],
                'date': '' if pd.isnull(df['transid']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            },
            {
                'category': '' if pd.isnull(df['sapreferenceno']) else 'SAP Document No.',
                'value': '' if pd.isnull(df['sapreferenceno']) else df['sapreferenceno'],
                'date': '' if pd.isnull(df['sapreferenceno']) else datetime.today().strftime('%Y-%m-%dT%H:%M:%S')
            }
        ]
    }

    logging.info('Proceeding to upload donations')
    params = delete_empty_keys(params)

    url = 'https://api.sky.blackbaud.com/gift/v1/gifts'

    post_request_re(url, params)


def delete_empty_keys(dictionary):
    new_dict = {}
    for k, v in dictionary.items():
        if isinstance(v, dict):  # If value is a dictionary, recurse
            v = delete_empty_keys(v)
        elif isinstance(v, list):  # If value is a list, iterate over each element
            v = [delete_empty_keys(item) if isinstance(item, dict) else item for item in v]
            v = [item for item in v if item]  # Remove empty items from the list
        if v:  # If the value is not empty, add it to the new dictionary
            new_dict[k] = v
    return new_dict


def get_campaign(desc):
    logging.info('Identifying the Campaign ID')

    camp_id = pd.read_sql_query(f"""
    SELECT id
        FROM campaign_list
        WHERE LOWER(description) = LOWER('{desc}');
    """, db_conn)

    # Case-Match statement to get Campaign ID
    match camp_id.shape[0]:

        # Found only one match
        case 1:
            return camp_id.loc[0, 'id']

        # Found no match
        case _:
            # Create a campaign
            return add_campaign(desc)


def add_campaign(desc):
    logging.info('Adding new campaign in Raisers Edge')

    camp_id = pd.read_sql_query(
        f'''
        SELECT MAX(campaign_id::INTEGER)
        FROM campaign_list
        WHERE campaign_id ~ '^[0-9]+$';
        ''',
        db_conn
    )

    camp_id = camp_id.loc[0, 'max'] + 1

    url = 'https://api.sky.blackbaud.com/nxt-data-integration/v1/re/campaigns'
    params = {
        'campaign_id': camp_id,
        'description': desc[:100]
    }

    # # URL encode the parameters
    params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)

    # Convert the encoded parameters string back to a dictionary
    params = dict(urllib.parse.parse_qsl(params))

    response = post_request_re(url, params)

    # Adding the new value to Database
    db_conn.execute(text(f"INSERT INTO campaign_list VALUES ('{camp_id}', '{desc}', '{response['id']}');"))
    db_conn.commit()

    return response['id']


def update_db(donation_id):
    logging.info(f'Updating DB that the donation has been uploaded for {donation_id}')

    # Adding the new value to Database
    db_conn.execute(text(f"INSERT INTO uploaded VALUES ({donation_id}, now());"))
    db_conn.commit()


def update_constituent(df, const_id):
    logging.info(f'Updating the constituent record: {const_id} for any updates')

    # Check if the donation is Online/Offline
    match df['provid']:

        # Online
        # Update Record as these are provided by Alum
        case 'PAYU' | 'SBIIB':

            # PAN Numbers
            # Check whether new PAN is valid
            if is_alphanumeric(df['pancard']):
                update_pan(df['pancard'], const_id)

            # Email Address
            if not df['email'].lower() in ['dean.acr.office@iitb.ac.in', 'dean.acr.office@gmail.com', 'donationrecipts@gmail.com',
                                 'donationreceipts@gmail.com']:
                update_email(df['email'], const_id)

            # Address
            update_address(df, const_id)

            # Education
            update_education(df, const_id)

            # Phone Number
            update_phones(df, const_id)

            # Name
            check_names(df, const_id)

        case _:
            # Compare only the name for Offline Donation
            logging.info('Checking if the Donor names match with the ones in Raisers Edge')
            check_names(df, const_id)


def check_names(df, const_id):
    logging.info('Checking names')

    new_name = df['name']

    url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{const_id}'
    params = {}

    api_response = get_request_re(url, params)

    if api_response['name'].split(' ')[-1].isdigit():
        re_name = ' '.join(api_response['name'].split(' ')[:-1])
    else:
        re_name = api_response['name']

    if new_name.strip().lower() != re_name.strip().lower():
        # New name doesn't match with the ones in RE

        logging.info('Sending email for different names')

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
                    <p>Hi,</p>
                    <p>This is to inform you that the name provided by Alum is different than that exists in Raisers Edge.</p>
                    <p>The new one has not been updated in Raisers Edge. You can manually review and update the same.</p>
                    <p><a href="https://host.nxt.blackbaud.com/constituent/records/{constituent_id}?envId=p-dzY8gGigKUidokeljxaQiA&amp;svcId=renxt" target="_blank"><strong>Open in RE</strong></a></p>
                    <table align="left" border="1" cellpadding="1" cellspacing="1" style="width:500px">
                        <thead>
                            <tr>
                                <th scope="col">Existing Name</th>
                                <th scope="col">New Name</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td style="text-align:center">{re_name}</td>
                                <td style="text-align:center">{new_name}</td>
                            </tr>
                        </tbody>
                    </table>
                    <p>&nbsp;</p>
                    <p>&nbsp;</p>
                    <p>&nbsp;</p>
                    <p>&nbsp;</p>
                    <p>Thanks &amp; Regards</p>
                    <p>A Bot.</p>
                    """

            # Create a text/html message from a rendered template
            email_body = template.format(
                constituent_id=const_id,
                re_name=re_name,
                new_name=new_name
            )

            if "access_token" in result:

                endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

                email_msg = {
                    'Message': {
                        'Subject': 'Different name exists in Raisers Edge than the ones provided by Donor',
                        'Body': {
                            'ContentType': 'HTML',
                            'Content': email_body
                        },
                        'ToRecipients': get_recipients(SEND_TO),
                        'CcRecipients': get_recipients(CC_TO),
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


def update_phones(df, const_id):
    logging.info('Proceeding to update phone')

    phone = df['contactno']

    if phone:

        # Get Phone numbers present in RE
        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{const_id}/phones'
        params = {}

        api_response = get_request_re(url, params)

        # Load to Dataframe
        re_data = pd.json_normalize(api_response['value'])

        if re_data.shape[0] != 0:
            # Check whether it exists
            # Remove non-numeric characters
            re_data['number'] = re_data['number'].apply(lambda x: re.sub(r'[^0-9]', '', x))
            new_phone = re.sub(r'[^0-9]', '', phone)

            phones_in_re = re_data['number'].to_list()

            # Adding last 10 characters of phone to the list as well
            counter = len(phones_in_re)
            i = 0

            for p in phones_in_re:
                i += 1
                phones_in_re.append(p[-10:])

                if i == counter:
                    break

            # If exists, mark as primary
            if new_phone in phones_in_re or new_phone[-10:] in phones_in_re:
                re_data = re_data[['id', 'number']].drop_duplicates('number').copy()

                phone_id = re_data[
                    re_data['number'].str.contains(new_phone[10:])
                ]['id'].values[0]

                url = f'https://api.sky.blackbaud.com/constituent/v1/phones/{int(phone_id)}'

                params = {
                    'primary': True,
                    'number': phone
                }

                patch_request_re(url, params)

                # Update Sync tags
                add_tags('Sync source', 'Donation', phone, const_id)

                # Update Verified Tags
                add_tags('Verified Phone', phone, 'Donation', const_id)

            # Else, add in RE
            else:
                params = {
                    'constituent_id': const_id,
                    'number': phone,
                    'primary': True,
                    'type': 'Mobile'
                }

                url = 'https://api.sky.blackbaud.com/constituent/v1/phones'

                post_request_re(url, params)

        # Else, add in RE
        else:
            params = {
                'constituent_id': const_id,
                'number': phone,
                'primary': True,
                'type': 'Mobile'
            }

            url = 'https://api.sky.blackbaud.com/constituent/v1/phones'

            post_request_re(url, params)


def update_education(df, const_id):
    logging.info('Proceeding to update Education')

    class_of = df['batch']
    department = df['dept']
    hostel = df['hostel']

    if class_of != '' or department != '' or hostel != '':
        # Get education present in RE
        url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{const_id}/educations'
        params = {}

        api_response = get_request_re(url, params)

        # Load to a dataframe
        re_data = pd.json_normalize(api_response['value'])

        # Check if any education data exists
        if not re_data.empty:

            re_data = re_data[re_data['school'] == 'Indian Institute of Technology Bombay'].reset_index(drop=True)

            if re_data.shape[0] == 1:

                education_id = int(re_data['id'][0])

                try:
                    re_class_of = int(re_data['class_of'][0])
                except:
                    re_class_of = class_of

                # Check if existing Class of is blank or invalid
                if class_of != '' and class_of == re_class_of:

                    url = f'https://api.sky.blackbaud.com/constituent/v1/educations/{education_id}'

                    params = {
                        'class_of': class_of,
                        'date_graduated': {
                            'y': class_of
                        },
                        'date_left': {
                            'y': class_of
                        },
                        'majors': [
                            department
                        ],
                        'social_organization': hostel
                    }

                    # Delete blank values from JSON
                    params = delete_empty_keys(params)

                    if params:
                        patch_request_re(url, params)

                        # Update Sync tags
                        education = str(class_of) + ', ' + str(department) + ', ' + str(hostel)
                        add_tags('Sync source', 'Donation',
                                 education.replace(', , ', ', ').strip()[:50], const_id)

                else:
                    # Different Education exists
                    send_mail_different_education(re_data, df,
                                                  'Different education data exists in RE and the one provided by Alum',
                                                  const_id)
            else:
                # Multiple education exists than what's provided
                re_data_html = re_data.to_html(index=False, classes='table table-stripped')
                each_row_html = df.to_html(index=False, classes='table table-stripped')
                send_mail_different_education(re_data_html, each_row_html, 'Multiple education data exists in RE',
                                              const_id)

        else:
            logging.info('Adding new education')

            # Upload Education
            url = 'https://api.sky.blackbaud.com/constituent/v1/educations'

            params = {
                'campus': department[:50],
                'class_of': class_of,
                'date_graduated': {
                    'y': int(class_of)
                },
                'date_left': {
                    'y': int(class_of)
                },
                'majors': [
                    department[:50]
                ],
                'primary': True,
                'school': 'Indian Institute of Technology Bombay',
                'social_organization': hostel[:50],
                'constituent_id': const_id
            }

            post_request_re(url, params)


def send_mail_different_education(re_data, each_row, subject, const_id):

    logging.info('Sending email for different education')

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
        <p>Hi,</p>
        <p>This is to inform you that the Education data provided by Alum is different than that exists in Raisers Edge.</p>
        <p><a href="https://host.nxt.blackbaud.com/constituent/records/{constituent_id}?envId=p-dzY8gGigKUidokeljxaQiA&amp;svcId=renxt" target="_blank"><strong>Open in RE</strong></a></p>
        <p>&nbsp;</p>
        <p>Below is the data for your comparison:</p>
        <h3>Raisers Edge Data:</h3>
        <p>{re_data}</p>
        <p>&nbsp;</p>
        <h3>Provided by Alum:</h3>
        <p>{education_data}</p>
        <p>&nbsp;</p>
        <p>Thanks &amp; Regards</p>
        <p>A Bot.</p>
        """

        # Create a text/html message from a rendered template
        email_body = template.format(
            constituent_id=const_id,
            re_data=re_data.to_html(index=False),
            education_data=pd.DataFrame(each_row).fillna('').T.to_html(index=False)
        )

        if "access_token" in result:

            endpoint = f'https://graph.microsoft.com/v1.0/users/{FROM}/sendMail'

            email_msg = {
                'Message': {
                    'Subject': subject,
                    'Body': {
                        'ContentType': 'HTML',
                        'Content': email_body
                    },
                    'ToRecipients': get_recipients(SEND_TO),
                    'CcRecipients': get_recipients(CC_TO),
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


def update_address(df, const_id):
    logging.info('Proceeding to update address')

    new_address = str(df['address1']) + ' ' + str(df['address2']) + ' ' + str(df['city']) + ' ' + str(df['state']) + ' ' + str(df['country']) + ' ' + str(df['zipcode'])
    logging.debug(new_address)
    new_address = new_address.replace(';', ' ').replace('\r\n', ' ').replace('\t', ' ').replace('\n', ' ').replace(
        'nan', ' ').replace('  ', ' ').strip()
    logging.debug(new_address)

    # Get addresses present in RE
    url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{const_id}/addresses'
    params = {}

    # API request
    api_response = get_request_re(url, params)

    # Load to dataframe
    address_df = pd.json_normalize(api_response['value'])

    # address_df['address'] = address_df[['address_lines', 'city', 'state', 'county', 'country', 'postal_code']].astype(
    #     str).apply(' '.join, axis=1)
    address_df['address'] = address_df['formatted_address'].apply(
        lambda x: str(x).replace('\r\n', ' ').replace('\t', ' ').replace('\n', ' ').replace('nan', ' ').replace('  ', ' ').strip())

    # Drop blank addresses
    re_address_list = address_df['address'].dropna().to_list()

    # Check if address exists
    if process.extractOne(new_address, re_address_list)[1] >= 95:
        # New address exists in RE, will check if it's primary

        # First let's identify the index
        address_id = int(
            address_df.loc[
                re_address_list.index(process.extractOne(new_address, re_address_list)[0]),
                ['id']
            ]['id']
        )

        url = f'https://api.sky.blackbaud.com/constituent/v1/addresses/{address_id}'

        params = {
            'preferred': True
        }

        patch_request_re(url, params)

        # Update Sync tags
        add_tags('Sync source', 'Donation', new_address[:50], const_id)

        # Update Verified Tags
        add_tags('Verified Location', new_address[:50], 'Donation', const_id)

    else:
        # New address doesn't exist in RE
        logging.info('Initialize Nominatim API for Geocoding')

        # initialize Nominatim API
        geolocator = Nominatim(user_agent="geoapiExercises")

        # adding 1 second padding between calls
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, return_value_on_exception=None)

        logging.info('Proceeding to update location')

        address_lines = ' ' if pd.isnull(df['address1']) else str(df['address1']) + ' ' + ' ' if pd.isnull(df['address2']) else str(df['address2'])
        city = ' ' if pd.isnull(df['city']) else str(df['city'])
        state = ' ' if pd.isnull(df['state']) else str(df['state'])
        country = ' ' if pd.isnull(df['country']) else str(df['country'])

        # Remove non-alphabetic characters
        city = re.sub('[^a-zA-Z ]+', '', city)
        state = re.sub('[^a-zA-Z ]+', '', state)
        country = re.sub('[^a-zA-Z ]+', '', country)

        if country != '' or ~(country == 'India' and city == '' and state == ''):
            address = str(address_lines) + ', ' + str(city) + ', ' + str(state) + ', ' + str(country)

            address = address.replace('nan', '').strip().replace(', ,', ', ')

            location = geolocator.geocode(address, addressdetails=True, language='en')

            while not location:
                print('I am here')

                address_split = address[address.index(' ') + 1:]
                address = address_split

                location = geolocator.geocode(address_split, addressdetails=True, language='en')

            address = location.raw['address']

            try:
                city = address.get('city', '')
                if city == '':
                    try:
                        city = address.get('state_district', '')
                        if city == '':
                            try:
                                city = address.get('county', '')
                            except:
                                city = ''
                    except:
                        try:
                            city = address.get('county', '')
                        except:
                            city = ''
            except:
                try:
                    city = address.get('state_district', '')
                    if city == '':
                        try:
                            city = address.get('county', '')
                        except:
                            city = ''
                except:
                    try:
                        city = address.get('county', '')
                    except:
                        city = ''

            state = address.get('state', '')
            country = address.get('country', '')

            url = 'https://api.sky.blackbaud.com/constituent/v1/addresses'

            # Ignore state for below countries
            if country == 'Mauritius' or country == 'Switzerland' or country == 'France' or country == 'Bahrain':
                state = ''

            params = {
                'address_lines': new_address.replace('  ', ' ').strip(),
                'city': city,
                'state': state,
                'county': state,
                'country': country,
                'postal_code': '' if pd.isnull(df['zipcode']) else int(df['zipcode']) if str(df['zipcode']).isdigit() else df['zipcode'],
                'constituent_id': const_id,
                'type': 'Home',
                'preferred': True
            }

            # Delete blank values from JSON
            params = delete_empty_keys(params)

            try:
                api_response = post_request_re(url, params)

                # Update Sync tags
                add_tags('Sync source', 'Donation', new_address[:50], const_id)

                # Update Verified Tags
                add_tags('Verified Location', new_address[:50], 'Donation', const_id)

            except:
                if 'county of value' in str(api_response).lower():
                    add_county(state)
                    post_request_re(url, params)

                    # Update Sync tags
                    add_tags('Sync source', 'Donation', new_address[:50], const_id)

                    # Update Verified Tags
                    add_tags('Verified Location', new_address[:50], 'Donation', const_id)

                else:
                    raise Exception(f'API returned an error: {api_response}')


def add_county(county):
    # counties = 5001
    # States = 5049
    i = 0
    code_table_ids = [5001, 5049]

    for code_table_id in code_table_ids:

        if i == 1:

            now = datetime.datetime.now()

            # Generate either a 2-digit or 3-digit number randomly
            if random.random() < 0.5:
                unique_num = int(now.strftime('%j%H%M%S%f')[:9])
            else:
                unique_num = int(now.strftime('%j%H%M%S%f')[:10])

            # Generate a random suffix character (either an alphabet or a special character)
            suffix_char = random.choice(string.ascii_lowercase + string.digits + '!@#$%^&*()')

            # Concatenate the unique number and the suffix character
            short_description = str(unique_num % 1000) + suffix_char

            if len(short_description) > 3:
                short_description = short_description[1:]

        else:
            short_description = ''

        url = f'https://api.sky.blackbaud.com/nxt-data-integration/v1/re/codetables/{code_table_id}/tableentries'

        params = {
            'long_description': county,
            'short_description': short_description
        }

        # Delete blank values from JSON
        params = delete_empty_keys(params)

        post_request_re(url, params)

        i += 1


def update_email(email, const_id):
    logging.info('Updating email address')

    # Check whether email exists in Raisers Edge
    url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{const_id}/emailaddresses'
    params = {}

    api_response = get_request_re(url, params)

    if email.lower() in [x['address'].lower() for x in api_response['value']]:
        # Email exists
        # Let's check if it's primary
        if not [True for x in api_response['value'] if
                (x['address'] == email.lower() and (x['primary'] == True or x['primary'] == 'True'))]:
            email_address_id = int([x['id'] for x in api_response['value'] if x['address'] == email.lower()][0])

            # Email address exists, but is not primary
            url = f'https://api.sky.blackbaud.com/constituent/v1/emailaddresses/{email_address_id}'
            params = {
                'address': email.lower(),
                'primary': True
            }

            patch_request_re(url, params)

    else:
        # Email doesn't exist
        url = 'https://api.sky.blackbaud.com/constituent/v1/emailaddresses'
        params = {
            'address': email,
            'constituent_id': const_id,
            'primary': True,
            'type': 'Email'
        }

        post_request_re(url, params)

    # Add Sync source
    add_tags('Sync source', 'Donation', email, const_id)

    # Add Verified tag
    add_tags('Verified Email', email, 'Donation', const_id)


def add_tags(category, value, comment, constituent_id):
    logging.info('Adding Tags to constituent record')

    params = {
        'category': category,
        'comment': comment,
        'parent_id': constituent_id,
        'value': value,
        'date': datetime.today().date().isoformat()
    }

    url = 'https://api.sky.blackbaud.com/constituent/v1/constituents/customfields'

    post_request_re(url, params)


def patch_request_re(url, params):

    logging.info('Running PATCH Request to RE function')

    # Request headers
    headers = {
        'Bb-Api-Subscription-Key': RE_API_KEY,
        'Authorization': 'Bearer ' + retrieve_token(),
        'Content-Type': 'application/json'
    }

    http.patch(url, headers=headers, data=json.dumps(params))


def update_pan(pan, const_id):
    logging.info('Updating PAN Numbers')

    # Check whether PAN exists in RE
    # Get Alias list
    url = f'https://api.sky.blackbaud.com/constituent/v1/constituents/{const_id}/aliases'
    params = {}

    api_response = get_request_re(url, params)

    if pan.lower() in [x['name'].lower() for x in api_response['value'] if x['type'] == 'Permanent Account Number']:
        logging.info('PAN Card already exists in Raisers Edge')

    else:
        url = 'https://api.sky.blackbaud.com/constituent/v1/aliases'
        params = {
            'constituent_id': const_id,
            'name': pan.upper(),
            'type': 'Permanent Account Number'
        }

        post_request_re(url, params)


def is_alphanumeric(s):
    if len(s) != 10:
        return False
    if not s.isalnum():
        return False
    if s.isalpha() or s.isdigit():
        return False
    return True


try:
    # Start Logging for Debugging
    start_logging()

    # Set current directory
    set_current_directory()

    # Load Environment Variables
    get_env_variables()

    # Load Donation Data
    donation_data = load_donation().copy()

    # Proceeding only if there's any donation data to upload
    if not donation_data.empty:

        # Connect to DataBase
        db_conn = connect_db()

        # Identify missing donations
        logging.info('Identifying pending donations')
        uploaded = get_from_db('uploaded').copy()

        new_donations = get_missing_donations().copy()

        # Upload Missing Data
        if new_donations.shape[0] > 0:

            # Set API Request strategy
            set_api_request_strategy()

            # Looping over each row in Dataframe
            for index, new_donation in new_donations.sample().iterrows():

                logging.info(f"Proceeding to update donation with Donation Portal Reference no.: {new_donation['dtlDonor_id']}")

                # Locate Donor
                re_id = locate_donor(new_donation)

                if re_id:

                    # Upload Donations
                    upload_donation(new_donation, re_id)

                    # Update DB
                    update_db(new_donation['dtlDonor_id'])

                    # Update constituent
                    update_constituent(new_donation, re_id)

                break

except Exception as Argument:

    logging.error(Argument)

    send_error_emails('Uploading Donation in Raisers Edge', Argument)

finally:

    # Closing DB connections
    disconnect_db()

    # # Housekeeping of donation files
    # housekeeping()

    # Stop Logging
    stop_logging()

    sys.exit()
