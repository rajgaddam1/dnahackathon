import streamlit as st
import os
import snowflake.connector
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from snowflake.connector.connection import SnowflakeConnection
from PIL import Image



##########Sidebar Logo
with st.sidebar:
    image = Image.open('Infosys_logo.JPG')
    st.write("Snowflake Hackathon ❄️")
    st.image(image)
    st.success("You are successfully logged in")

#############
##To manage bug in sreamlit(Intialize button click)
if 'key' not in st.session_state:
    st.session_state.key = False

def callback():
    st.session_state.key = True
    
###Function to convert data to csv

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

##############Snowflake Credentials
user = os.environ.get('user')
password = os.environ.get('password')
account = os.environ.get('account')

###Snow connection

con = snowflake.connector.connect(
                    user = user,
                    password = password,
                    account='MD93775.ap-southeast-1')

####Snowflake connection
def get_connector() -> SnowflakeConnection:
    """Create a connector to SnowFlake using credentials filled in Streamlit secrets"""
    con = snowflake.connector.connect(
    user = user,
    password = password,
    account = account,
    warehouse='DNAHACK')
    return con

snowflake_connector = get_connector()
#####Show warehouses
def get_wareshouse(_connector) -> pd.DataFrame:
    return pd.read_sql("SHOW WAREHOUSES;", _connector)

wareshouse = get_wareshouse(snowflake_connector)

list_ware = wareshouse['name'].to_list()
list_up = ['Create or Drop Warehouse']
list_ware_up = list_up + list_ware

##Snowflake Waarehouse dataframe to csv

ware_csv = convert_df(wareshouse)

#################Function to create Warehouse

def create_ware(con):
    ware_name = st.text_input('Enter Warehouse Name')
    ware_size = st.select_slider('Select size', ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'XLARGE', 'XXLARGE', 'XXXLARGE', 'X4LARGE', 'X5LARGE', 'X6LARGE'])
    sql_cmd = 'CREATE OR REPLACE WAREHOUSE  ' + str(ware_name) + ' ' +'WAREHOUSE_SIZE = '+ str(ware_size) +';'
    if st.button('Create Warehouse'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd)
            st.success('Warehouse has been created')
        except Exception as e:
            print(e)
            st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()
        
#Function to Drop Warehouse
def drop_ware(con, ware_name_del):
    #ware_name_del = st.radio("Select Warehouse to Drop",list_ware)
    sql_cmd = 'DROP WAREHOUSE IF EXISTS ' + str(ware_name_del) + ';'
    if st.button('Drop Warehouse'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd)
            st.success('Warehouse has been Dropped')
        except Exception as e:
            print(e)
            st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()




#################Function to create Databsase
def create_data(con):
    database_name = st.text_input('Enter Database Name')
    database_type = st.radio('Select Database Type', ['PERMANENT', 'TRANSIENT'])
    if database_type == 'PERMANENT':
        sql_cmd = 'CREATE OR REPLACE DATABASE  ' + str(database_name) + ';'
    else:
        sql_cmd = 'CREATE OR REPLACE TRANSIENT DATABASE  ' + str(database_name) + ';'
        
    if st.button('Create Database'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd)
            st.success('Database has been created')
        except Exception as e:
            print(e)
            st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()
        
    
################ SIDEBAR_1(WAREHOUSE)###########################
with st.sidebar:
    sel_ware = st.selectbox("Warehouse",list_ware_up)

###Action after selecting Database
if sel_ware != 'Create or Drop Warehouse':
    st.subheader('👇 Do you want to Drop '+ str(sel_ware) +' Warehouse? 🗑️')
    if st.button('Drop warehouse', on_click = callback) or st.session_state.key:
        
        drop_ware(con, sel_ware)

        #pass
    st.subheader('Warehouse Information')

    st.dataframe(wareshouse[['name', 'size']].loc[wareshouse['name'] == sel_ware])
"""    
    st.markdown("Click on below button to Download full Information about Warehouse")
    st.download_button(
    label = "Download data as CSV",
    data = ware_csv,
    file_name = 'Warehouse_info.csv',
    mime = 'text/csv',
)
"""
#### Homepage Create Warehouse
if sel_ware == 'Create or Drop Warehouse':
    st.title('Snowflake Hackathon ❄️')
    st.subheader("👇 Let's Create a Warehouse in Snowflake")
    
    if st.button('Create a new warehouse', on_click = callback) or st.session_state.key:
        create_ware(con)
    st.subheader("👇 Click here to Download full Information about Warehouses available")
    st.download_button(
    label = "Download data as CSV",
    data = ware_csv,
    file_name = 'Warehouse_info.csv',
    mime = 'text/csv',
)
    


####ShowDatabases
def get_databases(_connector) -> pd.DataFrame:
    return pd.read_sql("SHOW DATABASES;", _connector)

databases = get_databases(snowflake_connector)

##Snowflake Waarehouse dataframe to csv

database_csv = convert_df(databases)

##Adding Database type by creating copy of dataframe
databases_up = databases.copy()
databases_up.rename(columns={'options': 'type'}, inplace=True)
#databases_up['type'] = databases_up['type'].replace(np.nan, 'PERMANENT')
databases_up.type.fillna("PERMANENT",inplace = True)


list_data = databases['name'].to_list()
list_up = ['Select below available Databases']
list_data_up = list_up + list_data
#############SIDEBAR_2(DATABASES)
with st.sidebar:
    global sel_data
    sel_data = st.selectbox("Databases", list_data_up)
    
if sel_data != 'Select below available Databases':
    if st.button('Create a Database', on_click = callback) or st.session_state.key:
        
        create_data(con)
        #pass
    st.subheader('Database Information')

    st.dataframe(databases_up[['name', 'type']].loc[databases_up['name'] == sel_data])
    
    st.markdown("Click on below button to Download full Information about Database")
    st.download_button(
    label = "Download data as CSV",
    data = database_csv,
    file_name = 'Database_info.csv',
    mime = 'text/csv',
)


    
#############SIDEBAR_3(Schemas)  
def get_schema(_connector, dbname) -> pd.DataFrame:
    sql_cmd2 = 'SHOW SCHEMAS IN DATABASE ' + str(dbname) + ';'
    return pd.read_sql(sql_cmd2, _connector)
 
if sel_data != 'Select below available Databases':
    global sel_schema
    schemas_df = get_schema(snowflake_connector, sel_data)
    sc_list_data = schemas_df['name'].to_list()
    sc_list_up = ['Select below available Schemas']
    sc_list_data_up = sc_list_up + sc_list_data
    with st.sidebar:
        sel_schema = st.radio("Schema",sc_list_data_up)
