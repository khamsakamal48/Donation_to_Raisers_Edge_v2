# Donation_to_Raisers_Edge_v2

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
   pip install tensorflow
   pip install scikit-learn
   pip install nameparser
   pip install phonenumbers
   pip install fuzzywuzzy
   pip install python-Levenshtein
   pip install geopy
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
        dtlDonor_id int,
        date_uploaded date
    );
   
   CREATE TABLE constituent_list
    (
        constituent_id int,
        details character varying
    );
   
   CREATE TABLE campaign_list
    (
        campaign_id character varying,
        description character varying,
        id int
    );
   
   CREATE TABLE valid_constituents
    (
        id int
    );
   
   CREATE TABLE hf_gifts_to_upload
    (
        dtlDonor_id int
    );
   
   ```