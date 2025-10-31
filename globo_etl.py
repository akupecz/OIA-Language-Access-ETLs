import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST, AWS_DEV, metadata, globo_table
import fabric 
from paramiko.sftp_client import SFTPClient
import sftp_utils
import db_utils
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os 
import pandas as pd 
import numpy as np 

def main():
    '''
    ETL to extract GLOBO vendor data from SFTP. Connects to the server, grabs the file, saves a copy locally and to S3,
    generates an md5 hash and uploads unique rows to DataBridge.
    '''
    cgs.set_config(keeper_dir='~')
    sftp_client = cgs.connect_with_secrets(sftp_utils.create_sftp_conn, SFTP_SECRET, dir=GLOBO_DIRECTORY)
    num_files = sftp_utils.num_files_in_dir(sftp_client)

    if not num_files:
        print(f'No file found in directory: {GLOBO_DIRECTORY}')
    elif num_files > 1: 
        print(f'{num_files} files found in directory: {GLOBO_DIRECTORY}. Must only be 1.')
    else: 
        fname, ext = sftp_utils.get_sftp_fname(sftp_client)
        last_month = format(datetime.now() - relativedelta(months=1), '%B_%Y')
        local_name = 'GLOBO_' + last_month + ext

        sftp_utils.extract_sftp(sftp_client, fname, local_name)
        print(f"File successfully extracted as {local_name}!")

        # Read in excel and create md5 hash 
        df = pd.read_excel(local_name, sheet_name=1, skiprows=5)
        df.dropna(how='all', inplace=True) # A bunch of empty rows between data 
        df = df.astype({'PIN': str, 'Name': str}) # Needed to be able to sort 
        df = df.transform(np.sort)

        # Create counter column as proxy for unique rows 
        # There are identical rows in the data that need to be treated as distinct, this is our fix
        df['counter'] = range(1, len(df) + 1)
        df = db_utils.generate_md5_hash(df)
        df.drop('counter', axis=1, inplace=True)

        new_cols = ['service_date','client_name','language','service_detail','description','minutes_comments',
                    'total_charged','pin','name','md5_hash']
        df.columns = new_cols

        # Connect to DataBridge and insert
        databridge = cgs.connect_with_secrets(db_utils.connect_db, DB_SECRET_LOGIN, DB_SECRET_HOST)
        metadata.create_all(bind=databridge)
        with databridge.begin() as conn:
            db_utils.insert_in_batch(conn, globo_table, df.to_dict(orient='records'))
        
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