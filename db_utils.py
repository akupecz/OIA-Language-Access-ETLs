import citygeo_secrets as cgs
from config import SFTP_SECRET, GLOBO_DIRECTORY, ULG_DIRECTORY, DB_SECRET_LOGIN, DB_SECRET_HOST
import sqlalchemy as sa 
from sqlalchemy.dialects.postgresql import insert
import re
import pandas as pd 
import hashlib

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

def databridgeify(colname: str) -> str:
    '''
   
    '''
    colname = colname.lower()
    colname = re.sub(r' |\/', '_', colname)
    colname = re.sub(':|#', '', colname)
    # make sure repeated underscores aren't introduced by repeated calls on same column
    colname = re.sub(r'_+', '_', colname)
    colname = re.sub(r'^_', '', colname)
    return colname

def insert_in_batch(conn: sa.Connection, table: sa.Table, data: list, increment: int = 2000):
    '''
    Batch loads data into a specified table using supplied connection. 

    Arguments:
        conn     - SQLAlchemy connection object
        table    - SQLAlchemy Table object representing the target table
        data     - list of dictionaries representing the data to be inserted
        increment - number of rows to insert per batch (default is 2000)
    
    Returns: 
        None, but loads unique rows into the target table. Identical rows, ie those with the same md5 hash,
        will be ignored.
    '''
    start = 0
    row_count = len(data)
    while start <= row_count:
        end = start + increment
        data_to_insert = data[start:end]
        print(f'Inserting rows {start} to {min(end-1, row_count)}')
        stmt = insert(table).values(data_to_insert).on_conflict_do_nothing(index_elements=['md5_hash']) 
        result = conn.execute(stmt)
        start = end

    print(f'Appended {len(data):,} rows to {table.fullname}\n')


def generate_md5_hash(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Generates an md5 hash for each row in the dataframe by concatenating all columns
    into a single string, encoding it to utf-8, and then applying the md5 hash function.

    Arguments:
        df - pandas DataFrame for which to generate md5 hashes
    
    Returns:
        A pandas dataframe with the md5 hash column 
    '''
    df['all_concat'] = df.fillna('').astype(str).agg('|'.join, axis=1)
    df['md5_hash'] = df['all_concat'].apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
    df.drop('all_concat', axis=1, inplace=True)
    return df
