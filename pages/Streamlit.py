import streamlit as st
import os
import snowflake.connector
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from snowflake.connector.connection import SnowflakeConnection
#from PIL import Image

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
list_up = ['-------------------', 'Create a Warehouse']
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

    try:
        cur = con.cursor()
        cur.execute(sql_cmd)
        st.success('Warehouse has been Dropped')
    except Exception as e:
        print(e)
        #st.exception(e)
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
            #st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()
##############################
def clone_data(con):
    database_name1 = st.text_input('Enter Database Name')
    source_name = st.text_input('Enter Source Database Name')
    sql_cmd = 'CREATE OR REPLACE DATABASE ' + str(database_name1) + ' CLONE '+ str(source_name)  +';'
    if st.button('Done'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd)
            st.success('Database has been Cloned')
        except Exception as e:
            print(e)
            #st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()


       
#####Function to create Schema
def create_schema(con, dbname):
    schema_name = st.text_input('Enter Schema Name')
    schema_type = st.radio('Select Schema Type', ['PERMANENT', 'TRANSIENT'])
    if schema_type == 'PERMANENT':
        sql_cmd3 = 'CREATE OR REPLACE SCHEMA ' + str(dbname) + '.'  +str(schema_name) + ';'
    else:
        sql_cmd3 = 'CREATE OR REPLACE TRANSIENT SCHEMA '+ str(dbname) + '.'  +str(schema_name) + ';'
        
    if st.button('Create Schema'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd3)
            st.success('Schema has been created')
        except Exception as e:
            print(e)
            #st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()
    

        
#################Function to DROP Databsase       
def drop_database(con, database_name_del):
    #ware_name_del = st.radio("Select Warehouse to Drop",list_ware)
    sql_cmd = 'DROP DATABASE IF EXISTS ' + str(database_name_del) + ';'

    try:
        cur = con.cursor()
        cur.execute(sql_cmd)
        st.success('Database has been Dropped')
    except Exception as e:
        print(e)
        #st.exception(e)
        st.write('An error has occured please check logs')
    finally:
        cur.close()
    con.close()
    
############################Create Table/View  

def create_table(con):
    select_opt = st.radio('Create', ['None','Table', 'View'])
    if select_opt == 'Table':
        sql_cmd4 = st.text_input('Enter SQL Query', 'create table <table_name> (<col1_name> <col1_type>)')
    elif select_opt == 'View':
        sql_cmd4 = st.text_input('Enter SQL Query', 'create view <view_name> as <select_statement>;')
    if select_opt != 'None':
        if st.button('Create'):
            try:
                cur = con.cursor()
                cur.execute(sql_cmd4)
                st.success('Created')
            except Exception as e:
                print(e)
                #st.exception(e)
                st.write('Please Enter Valid Inputs')
            finally:
                cur.close()
            con.close()

################ SIDEBAR_1(WAREHOUSE)###########################
with st.sidebar:
    sel_ware = st.selectbox("Warehouse",list_ware_up)

###Action after selecting Warehouse
if sel_ware != 'Create a Warehouse' and sel_ware !=  '-------------------':
    st.subheader('👇 Do you want to Drop '+ str(sel_ware) +' Warehouse? 🗑️')
    if st.button('Drop warehouse'):
        
        drop_ware(con, sel_ware)

        #pass
    st.subheader('Warehouse Information')

    st.dataframe(wareshouse[['name', 'size']].loc[wareshouse['name'] == sel_ware])
   
    #st.markdown("Click on below button to Download full Information about Warehouse")
    #st.download_button(
    #label = "Download data as CSV",
    #data = ware_csv,
    #file_name = 'Warehouse_info.csv',
    #mime = 'text/csv',)

#### Homepage Create Warehouse
if sel_ware == 'Create a Warehouse':
    st.title('Snowflake Hackathon ❄️')
    st.subheader("👇 Let's Create a new Warehouse in Snowflake")
    
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
list_up = ['-------------------', 'Create a Database']
list_data_up = list_up + list_data

####SHOW SCHEMAS
def get_schema(_connector, dbname) -> pd.DataFrame:
    sql_cmd2 = 'SHOW SCHEMAS IN DATABASE ' + str(dbname) + ';'
    return pd.read_sql(sql_cmd2, _connector)



####SHOW TABLES
def get_table(_connector, dbname, scname) -> pd.DataFrame:
    sql_cmd3 = 'SHOW TABLES IN '+ str(dbname) + '.' + str(scname) + ';'
    return pd.read_sql(sql_cmd3, _connector)



######SHOW ROLES
def get_role(_connector) -> pd.DataFrame:
    return pd.read_sql("SHOW ROLES", _connector)

roles_df = get_role(snowflake_connector)

list_role = roles_df['name'].to_list()
list_up2 = ['-------------------', 'Create a Role']
list_role_up = list_up2 + list_role

role_csv = convert_df(roles_df)

#######SHOW USERS
def get_user(_connector) -> pd.DataFrame:
    return pd.read_sql("SHOW USERS", _connector)

users_df = get_user(snowflake_connector)

list_user = users_df['name'].to_list()
list_up3 = ['-------------------', 'Create a User']
list_user_up = list_up3 + list_user

user_csv = convert_df(users_df)

###################Function to display Query for copy

def show_query(_connector, dbname, scname, tables_df_query) -> pd.DataFrame:
    sel_table2 = st.radio("Table Available", tables_df_query.name)
    str1 = str(dbname)+ "." + str(scname) + "." + str(sel_table2)
    #cmd1 = "select get_ddl('table'," + f" '{str1}' " + ",True) As Query"
    cmd1 = "SELECT * FROM TABLE(DB1.PUBLIC.get_object_ddl1('table'," + f" '{str1}' "  +",true));"
    st.write(cmd1)
    #cmd1 =  "select * from get_ddl('table'," + f" '{str1}' " + ",True) As Query"
    #cmd1 = "select * from get_ddl('table'," + f" '{str1}' " + ",True) As Query"
    return pd.read_sql(cmd1, _connector)

########Publish Report 1
def get_report1(_connector) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM TABLE(DB1.PUBLIC.GET_PUBLISH_REPORT(-1));", _connector)

def get_report2(_connector) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM TABLE(DB1.PUBLIC.GET_PUBLISH_REPORT(-7));", _connector)

def get_report3(_connector) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM TABLE(DB1.PUBLIC.GET_PUBLISH_REPORT(-14));", _connector)



##### Function to create Role CREATE ROLE
def create_role(con):
    role_name = st.text_input('Enter Role Name')
    sql_cmd5 = 'CREATE OR REPLACE ROLE  ' + str(role_name) + ';'
    if st.button('Create Role'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd5)
            st.success('Role has been created')
        except Exception as e:
            print(e)
            #st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()

###Function to DROP ROLE
def drop_role(con, sel_role):
    #role_name = st.text_input('Enter Role Name')
    sql_cmd5 = 'DROP ROLE ' + str(sel_role) + ';'
    try:
        cur = con.cursor()
        cur.execute(sql_cmd5)
        st.success('Role has been Dropped')
    except Exception as e:
        print(e)
        #st.exception(e)
        st.write('An error has occured please check logs')
    finally:
        cur.close()
    con.close()

###### Function to create Role CREATE ROLE
def create_user(con):
    role_name = st.text_input('Enter User Name')
    #role_email = st.text_input('Enter Email Address')
    sql_cmd6 = "CREATE OR REPLACE USER " + str(role_name) + " PASSWORD = 'welcome' default_role = PUBLIC must_change_password = true" + ';'
    if st.button('Create User'):
        try:
            cur = con.cursor()
            cur.execute(sql_cmd6)
            st.success('User has been created. Plese note the default password is welcome and user must change password on first time login')
        except Exception as e:
            print(e)
            #st.exception(e)
            st.write('An error has occured please check logs')
        finally:
            cur.close()
        con.close()
###Function to DROP USER
def drop_user(con, sel_user):
    #role_name = st.text_input('Enter Role Name')
    sql_cmd5 = 'DROP USER ' + str(sel_user) + ';'
    try:
        cur = con.cursor()
        cur.execute(sql_cmd5)
        st.success('User has been Dropped')
    except Exception as e:
        print(e)
        #st.exception(e)
        st.write('An error has occured please check logs')
    finally:
        cur.close()
    con.close()


#############SIDEBAR_2(DATABASES)
with st.sidebar:
    global sel_data
    sel_data = st.selectbox("Databases", list_data_up)
    
###Create Databse Page
if sel_data == 'Create a Database':
    st.subheader("👇 Let's Create a new Database in Snowflake")
    
    if st.button('Create a new database', on_click = callback) or st.session_state.key:
        create_data(con)
    
    st.subheader('👇 Do you want to Clone Existing Database? 🗑️')
    agree1 = st.checkbox('Clone Database')
    if agree1:
        clone_data(con)
    st.subheader("👇 Click here to Download full Information about Databases available")
    st.download_button(
    label = "Download data as CSV",
    data = database_csv,
    file_name = 'Database_info.csv',
    mime = 'text/csv',
)
    

###Action after selecting Database    
if sel_data != 'Create a Database' and sel_data !=  '-------------------':
    global sel_schema
    #global table_df
    st.subheader('👇 Do you want to Drop '+ str(sel_data) +' Database?')
    if st.button('Drop Databse'):
        
        drop_database(con, sel_data)
        #pass
    #st.subheader('👇 Do you want to Clone Existing Database? 🗑️')
    #if st.button('Clone Databse'):
        #clone_data(con,sel_data)
    
    st.subheader('Database Information')

    st.dataframe(databases_up[['name', 'type']].loc[databases_up['name'] == sel_data])
    
    #st.markdown("Click on below button to Download full Information about Database")
    #st.download_button(label = "Download data as CSV",data = database_csv,file_name = 'Database_info.csv',mime = 'text/csv',)
    schemas_df = get_schema(snowflake_connector, sel_data)
    sc_list_data = schemas_df['name'].to_list()
    sc_list_up = ['Select below available Schemas']
    sc_list_data_up = sc_list_up + sc_list_data

    sel_schema = st.radio("Schemas Available",sc_list_data_up)

    
    st.subheader('Create a new Schema')
    if st.button('Create a new Schema', on_click = callback) or st.session_state.key:
        create_schema(con, sel_data)
    st.subheader('Create a Table/View')
    if sel_schema != 'Select below available Schemas':
        if st.button('Create a new Table/View', on_click = callback) or st.session_state.key:
            ###############Copy Query
            agree3 = st.checkbox('Copy query from existing Table')
            if agree3:
                
                tables_df_query = get_table(snowflake_connector, sel_data, sel_schema)
                show_query(con,sel_data,sel_schema,tables_df_query)
                query_df = show_query(snowflake_connector,sel_data,sel_schema, tables_df_query)
                st.dataframe(query_df)
            
            create_table(con)
    else:
        st.write('Select Schema to create table/view')
        
            
    
    if sel_schema != 'Select below available Schemas':
        tables_df = get_table(snowflake_connector, sel_data, sel_schema)
        if len(tables_df) != 0:
            sel_table = st.radio("Tables Available", tables_df.name)
        else:
            st.write('No tables available')
        
#############SIDEBAR_3(Roles)
with st.sidebar:
    global sel_role
    sel_role = st.selectbox("Role", list_role_up)
    
if sel_role == 'Create a Role':
    
    st.subheader("👇 Let's Create a new Role in Snowflake")
    if st.button('Create a new Role', on_click = callback) or st.session_state.key:
        create_role(con)
        
    st.subheader("👇 Click here to Download full Information about Roles available")
    st.download_button(
    label = "Download data as CSV",
    data = role_csv,
    file_name = 'Roles_info.csv',
    mime = 'text/csv',)

if sel_role != 'Create a Role' and sel_role != '-------------------':
    st.subheader('👇 Do you want to Drop '+ str(sel_role) +' Role?')
    if st.button('Drop Role'):
        drop_role(con, sel_role)
        
    st.subheader('Role Information')

    st.dataframe(roles_df[['name', 'comment']].loc[roles_df['name'] == sel_role])
    

#######SIDEBAR_4(USERS)
with st.sidebar:
    global sel_user
    sel_user = st.selectbox("User", list_user_up)
    
if sel_user == 'Create a User':
    
    st.subheader("👇 Let's Create a new User in Snowflake")
    if st.button('Create a new User', on_click = callback) or st.session_state.key:
        create_user(con)
        
    st.subheader("👇 Click here to Download full Information about Users available")
    st.download_button(
    label = "Download data as CSV",
    data = user_csv,
    file_name = 'User_info.csv',
    mime = 'text/csv',)

if sel_user != 'Create a User' and sel_user != '-------------------' :
    
    st.subheader('👇 Do you want to Drop User '+ str(sel_user) +'?')
    if st.button('Drop User'):
        drop_user(con, sel_role)
        
    st.subheader('User Information')

    st.dataframe(users_df[['name', 'has_password']].loc[users_df['name'] == sel_user])

#######SIDEBAR_4(USERS)
with st.sidebar:
    global sel_report
    sel_report = st.selectbox('Reports', ['-------------------', 'Get Publish Report'])

if sel_report == 'Get Publish Report':
    st.subheader('Publish Report')
    sel_days = st.radio("Get Objects Created or Modified", ['None','Last Day', 'Last 7 Days', 'Last 14 days'])
    if sel_days == 'Last Day':
        report1_df = get_report1(snowflake_connector)
        st.dataframe(report1_df)
    if sel_days == 'Last 7 Days':
        report2_df = get_report2(snowflake_connector)
        st.dataframe(report2_df)
    if sel_days == 'Last 14 days':
        report3_df = get_report3(snowflake_connector)
        st.dataframe(report3_df)    
    
    



    
