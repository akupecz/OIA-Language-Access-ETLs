# NEED TO CHANGE SCHEMA FOR THE SA TABLES BELOW!!!!

import sqlalchemy as sa 
#######################################
############ SFTP Vars ################
#######################################
SFTP_SECRET = 'SFTP Server - CityGeo'
GLOBO_DIRECTORY = 'OIA-LA-GLOBO-Reports'
ULG_DIRECTORY= 'OIA-LA-ULG -Reports'
POWERLING_DIRECTORY = 'IDK YET!!!!!'
LSA_DIRECTORY= 'OIA-LA-GLOBO-Reports' ############# NEED TO CHANGE THIS!!!!!!

#######################################
########## Databridge Vars ############
#######################################
# DB_SECRET_LOGIN = 'databridge-v2/citygeo'
# DB_SECRET_HOST = 'databridge-v2/hostname'
# DB_SECRET_HOST_TEST = 'databridge-v2/hostname-testing'
DB_SECRET_LOGIN = 'ps360-DB'
DB_SECRET_HOST = 'databridge'

#######################################
############ AWS Vars #################
#######################################
AWS_DEV = 'AWS Dev'
AWS_BUCKET = 'citygeo-airflow-databridge2'

metadata = sa.MetaData() 

#######################################
################# ULG #################
#######################################
ulg_table = sa.Table(
    'ulg_lang_access_raw',
    metadata, 
    sa.Column('call_id', sa.Integer),
    sa.Column('call_date', sa.Date),
    sa.Column('call_time', sa.Text),
    sa.Column('client', sa.Text),
    sa.Column('client_id', sa.Integer),
    sa.Column('prompt_1', sa.Text),
    sa.Column('prompt_2', sa.Text),
    sa.Column('prompt_3', sa.Text),
    sa.Column('client_phone', sa.Text),
    sa.Column('program_code', sa.Text),
    sa.Column('language', sa.Text),   
    sa.Column('interpreter_id', sa.Integer),   
    sa.Column('ict', sa.Float),   
    sa.Column('minutes', sa.Float),   
    sa.Column('minute_rate', sa.Float),   
    sa.Column('amount', sa.Float),   
    sa.Column('invoice_num', sa.Integer),   
    sa.Column('md5_hash', sa.Text, primary_key=True),   
    schema='philly_stat_360'
)


# #######################################
# ############### GLOBO #################
# #######################################
globo_table = sa.Table(
    'globo_lang_access_raw',
    metadata, 
    sa.Column('service_date', sa.Date),
    sa.Column('client_name', sa.Text),
    sa.Column('language', sa.Text),
    sa.Column('service_detail', sa.Text),
    sa.Column('description', sa.Text),
    sa.Column('minutes_comments', sa.Float),
    sa.Column('total_charged', sa.Float),
    sa.Column('pin', sa.Text),
    sa.Column('name', sa.Text),  
    sa.Column('md5_hash', sa.Text, primary_key=True),   
    schema='philly_stat_360'
)

# #######################################
# ################# LSA #################
# #######################################
lsa_table = sa.Table(
    'lsa_lang_access_raw',
    metadata, 
    sa.Column('record_number', sa.Text),
    sa.Column('account_code', sa.Integer), 
    sa.Column('account_name', sa.Text),
    sa.Column('language', sa.Text),
    sa.Column('interpreter_id', sa.Integer),
    sa.Column('start_date', sa.Date),
    sa.Column('start_time', sa.Time),
    sa.Column('length', sa.Float),
    sa.Column('ani', sa.Text),
    sa.Column('device_id', sa.Text),
    sa.Column('intake_1', sa.Text),   
    sa.Column('description', sa.Text),   
    sa.Column('intake_2', sa.Text),   
    sa.Column('intake_3', sa.Text),   
    sa.Column('intake_4', sa.Text),   
    sa.Column('intake_5', sa.Text),    
    sa.Column('third_party_length', sa.Text),   
    sa.Column('third_party_charge', sa.Float),   
    sa.Column('third_party_country', sa.Text),   
    sa.Column('total_charge', sa.Float),   
    sa.Column('md5_hash', sa.Text, primary_key=True), 
    schema='philly_stat_360'
)
# #######################################
# ############## Powerling ##############
# #######################################
# powerling_table = sa.Table(
#     'table name here',
#     metadata, 
#     sa.Column('col name', sa.Integer),
#     schema='citygeo'
# )