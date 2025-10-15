import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST, AWS_DEV
import fabric 
from paramiko.sftp_client import SFTPClient
import sftp_utils
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os 

def main():
    cgs.set_config(keeper_dir='~')
    sftp_client = cgs.connect_with_secrets(sftp_utils.create_sftp_conn, SFTP_SECRET, dir=GLOBO_DIRECTORY)

    # Check if there is a file to act on
    if sftp_utils.check_file_existence:
        # Get the filename and extension of file located in directory 
        fname, ext = sftp_utils.get_sftp_fname(sftp_client)
        
        # Set up local file name 
        last_month = format(datetime.now() - relativedelta(months=1), '%B_%Y')
        local_name = 'GLOBO_' + last_month + ext

        # Now extract file 
        sftp_utils.extract_sftp(sftp_client, fname, local_name)
        print("File successfully extracted!")

        # Process the file (not needed if going dbt route) 

        # Load to databridge 


        # Remove local copy
        os.remove(local_name)

    else:
        print(f'No file found in directory: {GLOBO_DIRECTORY}')
    

    # Cleanup 
    print('Exiting...')
    sftp_client.close()

if __name__ == '__main__': 
    main()