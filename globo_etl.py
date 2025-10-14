import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST
import fabric 
from paramiko.sftp_client import SFTPClient
import utils

def main():
    cgs.set_config(keeper_dir='~')
    # Create sftp connection at specified directory 
    sftp_client = utils.setup_sftp(GLOBO_DIRECTORY)


    return

if __name__ == '__main__': 
    main()