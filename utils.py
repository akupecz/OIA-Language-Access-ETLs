import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, ULG_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST
import fabric 
from paramiko.sftp_client import SFTPClient

def create_sftp_conn(creds: dict) -> fabric.Connection:
    '''
    Creates and returns sftp connection. Note 'look_for_keys' in the Connection constructor 
    is set to False; this is needed to use the password instead of ssh key. 

    Arguments:
        creds - dictionary containing the credentials to connect to sftp server
    
    Returns: 
        Returns an sftp connection
    '''
    sftp_creds = creds[SFTP_SECRET]
    host, user, password = sftp_creds['Host'], sftp_creds['login'], sftp_creds['password']

    sftp_conn = fabric.Connection(
        host=host, 
        user=user,
        connect_kwargs={'password': password, 'look_for_keys': False} 
    )

    return sftp_conn



# Func to navigate in the sftp connection 
def setup_sftp(dir: str = None) -> SFTPClient:
    '''
    Creates paramiko SFTP Client and changes "working directory" to specified path. 
    If no directory is specified, will default to home directory.
    If specified directory DNE, then IOError will be thrown. 

    Arguments:
        dir - String representing the directory to change into. Directory must exist on server
    
    Returns: 
        SFTPClient with a cwd of whatever is supplied. 
    '''
    sftp_conn = cgs.connect_with_secrets(create_sftp_conn, SFTP_SECRET)
    sftp_client = sftp_conn.sftp()

    if dir:
        try:
            sftp_client.chdir(dir)
        except IOError:
            print(f"Specified directory, {dir}, does not exist on server!")
            sftp_client.close()
    
    return sftp_client

# Func to grab file from SFTP connection and download locally 


# Func to move local to S3 


# Func to connect to DB 


# Function to load local into databridge 


