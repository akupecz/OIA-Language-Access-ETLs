import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, ULG_DIRECTORY, AWS_DEV, AWS_BUCKET
import fabric 
from paramiko.sftp_client import SFTPClient
import boto3 
import re

def create_sftp_conn(creds: dict, dir: str = None) -> SFTPClient:
    '''
    Creates and returns paramiko SFTP Client. If dir is specified, will change directory
    to dir. If no directory is specified, will default to the home directory. If specified directory
    DNE, then IOError will be thrown. 
     
    Note: 'look_for_keys' in the Connection constructor is set to False; this
    is needed to use the password instead of ssh key. 

    Arguments:
        creds - dictionary containing the credentials to connect to sftp server
        dir   - path to change directories to. 
    
    Returns: 
        SFTPClient with a cwd of whatever is supplied.
    '''
    sftp_creds = creds[SFTP_SECRET]
    host, user, password = sftp_creds['Host'], sftp_creds['login'], sftp_creds['password']

    sftp_conn = fabric.Connection(
        host=host, 
        user=user,
        connect_kwargs={'password': password, 'look_for_keys': False} 
    )
    sftp_client = sftp_conn.sftp()

    if dir:
        try:
            sftp_client.chdir(dir)
        except IOError:
            print(f"Specified directory, {dir}, does not exist on server!")
            sftp_client.close()

    return sftp_client

def get_sftp_fname(sftp_client: SFTPClient) -> tuple[str, str]:
    '''
    Returns the file name and extension of item located in sftp server in a specified directory.
    Assumes there is only one file located in the directory. 

    Arguments:
        sftp_client - the sftp client set on a specific directory

    Returns:
        A tuple whose first element is the full filename and second element is the file extension.  
    '''
    fname = sftp_client.listdir_attr()[0].filename
    file_extension = re.search(r'(\.[^.]+)$', fname).group(1)

    return fname, file_extension

def extract_sftp(sftp_client: SFTPClient, file: str, localname: str):
    '''
    Downloads file from sftp server, pulling from the directory set in
    the setup_sftp function, to local directory with name localname.
    Also uploads file to s3  

    Arguments:
        sftp_client - the sftp client set on a specific directory 
        file        - the name of the file we are extracting 
        localname   - the name we wish to save the downloaded file as 
    
    Returns:
        Nothing, but downloads local copy of file 
    '''
    sftp_client.get(file, localname)
    s3_conn = cgs.connect_with_secrets(setup_s3, AWS_DEV)
    add_to_s3(s3_conn, file, localname)

def setup_s3(creds: dict):
    '''
    Sets up boto3 S3 client using the specified credentials 

    Arguments:
        creds - dictionary containing the access key and secret key to connect to AWS 

    Returns:
        boto3 s3 connection
    '''
    return boto3.client(
        's3',
        aws_access_key_id = creds[AWS_DEV]['access_key'],
        aws_secret_access_key = creds[AWS_DEV]['secret_key']
    )


def add_to_s3(conn, fname: str, key: str):
    '''
    Uploads local file to S3 bucket. 

    Arguments:
        conn - the boto3 s3 client 
        fname - the filename of the local file to upload
        key - the name the file will be called in S3 
    '''
    try: 
        conn.upload_file(Filename=fname, Bucket=AWS_BUCKET, Key=key)
        print(f'Successfully wrote "{key}" to S3 Bucket "{AWS_BUCKET}"')
    except (conn.exceptions.ClientError, conn.exceptions.NoSuchBucket) as e: 
        print(f'Error writing "{key}" to S3 Bucket "{AWS_BUCKET}"')
        raise e    


def check_file_existence(sftp_client: SFTPClient) -> bool:
    '''
    Checks if there is a file to download from server in the directory specified when connecting to sftp. 
    Returns true only for sizes > 0 

    Arguments:
        sftp_client - the sftp client set on a specific directory 
   
    Returns:
    True {x | x != 0}, False otherwise 
    '''
    return len(sftp_client.listdir_attr())










# def extract_sftp(sftp_client: SFTPClient, file: str, localname: str):
#     '''
#     Downloads file from sftp server, pulling from the directory set in
#     the setup_sftp function, to local directory and uploads to S3. 
#     Names the downloaded file as <Vendor>_<Month>_<Year>.<file extension>.

#     Arguments:
#         sftp_client - the sftp client set on a specific directory 
#         vendor      - the name of the vendor we are pulling data from (ULG, LSA, GLOBO, Powerling)
#         file        - the name of the file we are extracting 
    
#     Returns:
#         Nothing, but will upload the file to S3 
#     '''

#     # Get file name and extension
#     fname = sftp_client.listdir_attr()[0].filename
#     file_extension = re.search(r'(\.[^.]+)$', fname).group(1)



#     # Download 
#     sftp_client.get(fname, local_name)




#     s3_conn = cgs.connect_with_secrets(setup_s3, AWS_DEV)

#     # Expects that there will only ever be one file in the directory at a time 

#     sftp_client.get(fname, fname)

#     # move to S3 and remove local download
#     add_to_s3(s3_conn, fname, vendor.upper() + "_" + last_month + file_extension)
#     os.remove(fname)