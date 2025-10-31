import citygeo_secrets as cgs
from config import SFTP_SECRET, LSA_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST, AWS_DEV, metadata, lsa_table
import fabric 
from paramiko.sftp_client import SFTPClient
import sftp_utils
import db_utils
import boto3
import datetime
from dateutil.relativedelta import relativedelta
import os 
import pandas as pd 
import hashlib

def main():
    '''
    ETL to extract LSA vendor data from SFTP. Connects to the server, grabs the file, saves a copy locally and to S3,
    generates an md5 hash and uploads unique rows to DataBridge.
    '''
    cgs.set_config(keeper_dir='~')
    sftp_client = cgs.connect_with_secrets(sftp_utils.create_sftp_conn, SFTP_SECRET, dir=LSA_DIRECTORY) 
    num_files = sftp_utils.num_files_in_dir(sftp_client)

    if not num_files:
        print(f'No file found in directory: {LSA_DIRECTORY}')
    elif num_files > 1: 
        print(f'{num_files} files found in directory: {LSA_DIRECTORY}. Must only be 1.')
    else: 
        fname, ext = sftp_utils.get_sftp_fname(sftp_client)
        last_month = format(datetime.datetime.now() - relativedelta(months=1), '%B_%Y')
        local_name = 'LSA' + last_month + ext

        sftp_utils.extract_sftp(sftp_client, fname, local_name)
        print(f"File successfully extracted as {local_name}!")

        # Read in excel and create md5 hash
        df = pd.read_excel(local_name, sheet_name=0)
        df.dropna(subset=['Account Code'], inplace=True) # Extra rows at bottom 
        df = db_utils.generate_md5_hash(df)
        new_cols = ['record_number','account_code','account_name','language','interpreter_id','start_date',
                    'start_time','length','ani','device_id','intake_1','description','intake_2','intake_3',
                    'intake_4','intake_5','third_party_length','third_party_charge','third_party_country','total_charge','md5_hash']
        df.columns = new_cols
        df['length'] = pd.to_datetime(df['length'],format= '%H:%M:%S', errors='ignore')
        df['length'] = df['length'].apply(lambda x: datetime.timedelta(hours=x.hour, minutes=x.minute, seconds=x.second).total_seconds()/60)
        
        # Connect to DataBridge and insert
        databridge = cgs.connect_with_secrets(db_utils.connect_db, DB_SECRET_LOGIN, DB_SECRET_HOST)
        metadata.create_all(bind=databridge)
        with databridge.begin() as conn:
            db_utils.insert_in_batch(conn, lsa_table, df.to_dict(orient='records'))
        
        # Remove file from local and sftp 
        sftp_client.remove(fname)
        print(f'Removed {fname} from SFTP server.')
        os.remove(local_name)
        print(f'Removed {local_name} from local storage.')
    

    # Cleanup 
    print('Exiting...')
    sftp_client.close()
    print('Connection has been closed.')

if __name__ == '__main__': 
    main()