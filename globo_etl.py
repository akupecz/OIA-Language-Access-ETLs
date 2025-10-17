import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST, AWS_DEV, metadata
import fabric 
from paramiko.sftp_client import SFTPClient
import sftp_utils
import db_utils
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os 

def main():
    cgs.set_config(keeper_dir='~')
    sftp_client = cgs.connect_with_secrets(sftp_utils.create_sftp_conn, SFTP_SECRET, dir=GLOBO_DIRECTORY)

    if sftp_utils.check_file_existence(sftp_client):
        fname, ext = sftp_utils.get_sftp_fname(sftp_client)
        last_month = format(datetime.now() - relativedelta(months=1), '%B_%Y')
        local_name = 'GLOBO_' + last_month + ext

        sftp_utils.extract_sftp(sftp_client, fname, local_name)
        print("File successfully extracted and uploaded to S3!")

        # Process the file (not needed if going dbt route) 
        # Load to database 

        # Remove local copy
        os.remove(local_name)
    else:
        print(f'No file found in directory: {GLOBO_DIRECTORY}')
    
    # Cleanup 
    print('Closing sftp connection...')
    sftp_client.close()

    # Do databridge stuff with local file:
    db_engine = cgs.connect_with_secrets(db_utils.connect_db, DB_SECRET_HOST, DB_SECRET_LOGIN)
    with db_engine.begin() as conn: 
        # Create table if not exists 
        print(f"Creating table: {TARGET_TABLE}")
        metadata.create_all(bind=conn)
        print(f"Table {TARGET_TABLE} created!")



if __name__ == '__main__': 
    main()