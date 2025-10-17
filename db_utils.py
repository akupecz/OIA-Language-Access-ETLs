import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, ULG_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST
import sqlalchemy as sa 

def connect_db(creds: dict) -> sa.engine.Engine:
    '''
    Connects to databridge using the supplied credentials.

    Arguments:
        creds - dictionary containing access credentials 
    
    Returns: 
        SQLAlchemy engine
    '''
    url_object = sa.URL.create(
        drivername='postgresql+psycopg',
        username=creds[DB_SECRET_LOGIN]["login"],
        password=creds[DB_SECRET_LOGIN]["password"],
        host=creds[DB_SECRET_HOST]["host"],
        port=creds[DB_SECRET_HOST]["port"],
        database=creds[DB_SECRET_HOST]["database"]
    )
    
    engine = sa.create_engine(url_object)
    engine.connect()
    return engine 



# Func to move local to S3 



# Function to load local into databridge 


