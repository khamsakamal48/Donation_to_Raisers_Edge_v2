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

from dotenv import load_dotenv
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from nameparser import HumanName
from tensorflow import keras
from sklearn.metrics import f1_score


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
        ~donation_data['dtlDonor_id'].isin(uploaded['dtldonor_id'])
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

    const_id = pd.read_sql_query(
        f'''SELECT DISTINCT constituent_id
        FROM constituent_list
        WHERE LOWER(details) IN (LOWER('{email}'), '{phone}');
        ''',
        db_conn
    )

    # Case-Match statement to get RE ID
    match const_id.shape[0]:

        # Found only one match
        case 1:
            return const_id.loc[0, 'constituent_id']

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
            print('Found multiple matches')
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
            df=df
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
                    'ToRecipients': get_recipients(SEND_TO)
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

    if pd.isnull(company_name) | affiliation != 'Foundation':
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

        title = str(name.title).title()

        # Get Gender
        gender = get_gender(first_name)

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
        <h2><a href="{url}" target="_blank"><span style="color: #ffffff;">Open in RE</span></a></h2>
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
            url=f'https://host.nxt.blackbaud.com/constituent/records/{const_id}?envId=p-dzY8gGigKUidokeljxaQiA&svcId=renxt',
            df=df,
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
                    'ToRecipients': get_recipients(SEND_TO)
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
    }

    re_api_response = http.post(url, params=params, headers=headers, json=params).json()

    return re_api_response


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

    aux = np.zeros(len_vocab);
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
    logging.info(f'Uploading donation for RE ID: {const_id}')




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

                    # Upload Receipts
                    # Upload Thank You Letters

                    # Update DB

                break

except Exception as Argument:

    logging.error(Argument)

    send_error_emails('Uploading Donation in Raisers Edge', Argument)

finally:

    # Closing DB connections
    disconnect_db()

    # # Housekeeping of donation files
    # housekeeping()

    exit()
