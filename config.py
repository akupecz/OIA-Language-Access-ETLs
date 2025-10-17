import sqlalchemy as sa 

# SFTP Vars
SFTP_SECRET = 'SFTP Server - CityGeo'
GLOBO_DIRECTORY = 'OIA-LA-GLOBO-Reports'
ULG_DIRECTORY= 'OIA-LA-ULG -Reports'
POWERLING_DIRECTORY = 'IDK YET!!!!!'
LSA_DIRECTORY= 'IDK YET EITHER!!!'

# Databridge Vars 
# DB_SECRET_LOGIN = 'databridge-v2/citygeo'
# DB_SECRET_HOST = 'databridge-v2/hostname'
# DB_SECRET_HOST_TEST = 'databridge-v2/hostname-testing'
DB_SECRET_LOGIN = 'ps360-DB'
DB_SECRET_HOST = 'databridge'

# AWS Vars
AWS_DEV = 'AWS Dev'
AWS_BUCKET = 'PUT BUCKET HERE'

# Table stuff 
metadata = sa.MetaData() 

globo_table = sa.Table(
    'table name here',
    metadata, 
    sa.Column('col name', sa.Integer),
    schema='citygeo'
)

lsa_table = sa.Table(
    'table name here',
    metadata, 
    sa.Column('col name', sa.Integer),
    schema='citygeo'
)

ulg_table = sa.Table(
    'table name here',
    metadata, 
    sa.Column('col name', sa.Integer),
    schema='citygeo'
)

powerling_table = sa.Table(
    'table name here',
    metadata, 
    sa.Column('col name', sa.Integer),
    schema='citygeo'
)