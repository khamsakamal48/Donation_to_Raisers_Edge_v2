# Donation_to_Raisers_Edge_v2

## To do
- Create database
  - Uploaded IDs
  - Userbase of Email, PAN and Phone number
  - Projects
- Searches:
  - Let it search the DB first and then call API
  - Ensure that only result is matched
  - Else email
  - Search campaign else create it automatically

## Pre-requisites
1. Install below packages
    ``` bash
   pip install sqlalchemy
   pip install psycopg2-binary
   pip install paramiko
   pip install python-dotenv
   pip install msal
   pip install pandas
   pip install numpy
   pip install chardet
   ```
2. If you encounter error on installing pyscopg2, then try:
    ``` bash
    pip install psycopg2-binary
   ```

## Deployment steps
1. Setup a PostgreSQL database and load below tables
    ``` sql
   CREATE DATABASE "donation-to-re"
   
   CREATE TABLE uploaded
    (
        dtlDonor_id int
    );
   
   CREATE TABLE constituent_list
    (
        constituent_id int,
        details character varying
    );
   
   ```
2. 