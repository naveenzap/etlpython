import pandas as pd
import psycopg2
import urllib.parse
import time
import matplotlib.pyplot as plt
import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
import pyodbc


password_file_path = r'C:\Users\DELL\Desktop\postgres_password\password.txt'

with open(password_file_path, 'r') as file:
    password = file.read().strip()



#encode the passsword
encoded_password = urllib.parse.quote_plus(password)

#Server=localhost\SQLEXPRESS;Database=master;Trusted_Connection=True;


#default server
#server = 'localhost\\SQLEXPRESS'
server = 'DESKTOP-F54N72P\\SQLEXPRESS'
Database = 'players'


import subprocess




# Assuming a TCP rule for port 1433 (SQL Server default)
rule_name = "Allow_SQL_Server_Access"
port = 1433
command = f'netsh advfirewall firewall add rule name="{rule_name}" dir=in action=allow protocol=TCP localport={port}'

try:
    subprocess.run(command, shell=True, check=True)
    print(f"Firewall rule '{rule_name}' created for port {port}.")
except subprocess.CalledProcessError as e:
    print("Error creating firewall rule:", e)



#connection_string = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER="+server+";DATABASE="+Database
#connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={Database};Trusted_Connection=yes' #;UID={username};PWD={password}'

#Construct the SQLAlchemy connection URL
connection_url = f"mssql+pyodbc://{server}/{Database}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
print(connection_url)

#Remote Access - SQL Server - O refers not-enabled & 1 refers enabled
#SELECT value FROM sys.configurations WHERE name LIKE 'remote access';
from sqlalchemy import create_engine, MetaData

# ... (your connection details)

engine = create_engine(connection_url)

#SELECT name FROM sys.databases;

df = pd.read_sql_query("SELECT name FROM sys.databases;", engine)
print("Successfully retrieved data from 'databases' table!")
df.to_excel("databases.xlsx")

# Create a MetaData object to reflect database schema
metadata = MetaData()

try:
    # Check if table exists (optional)
    metadata.reflect(engine, only=['cricketers'])  # Replace with actual table name
    table_exists = 'cricketers' in metadata.tables

    if table_exists:
        df1 = pd.read_sql_query("SELECT * FROM dbo.cricketers", engine)
        df1.to_excel("cricketers.xlsx")
        print(df1)
        print("Successfully retrieved data from 'cricketers' table!")
    else:
        print("Table 'cricketers' not found in the database.")

except Exception as ex:
    print("Error:", ex)
finally:
    engine.dispose()
    print("Connection to SQL Server closed.")

'''
if not is_admin():
    print("This script requires administrator privileges to create the firewall rule.")
    # Code to elevate privileges (e.g., using a batch file)
    # ...
else:
    # Create firewall rule using netsh (assuming correct connection string)
    # ...
    try:
        #conn_sql = pyodbc.connect(connection_string)
        # ... (your connection string definition)
        engine = create_engine(connection_url)
        # Your SQL operations here
        df = pd.read_sql_query("""select * from cricketers; """, connection_url)
        df.to_excel("cricketers.xlsx")
    except pyodbc.Error as ex:
        print("Database connection error:", ex) 

    finally:
        if conn_sql:
            conn_sql.close() 
    
'''


#df = pd.read_sql_query("""select * from cricketers; """,conn_sql)
#df.to_excel("cricketers.xlsx")

# Replace the following with your actual database details
dbname = "sqlauto"
user = "postgres"
password = "12345"
host = "localhost"
port = 5432



# Connect to the database
#conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

'''

engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
)

'''

#O/P
#C:\Users\DELL\Desktop\PycharmProjects\venv\Scripts\python.exe C:\Users\DELL\Desktop\PycharmProjects\pythonProject\venv\sqlserver_conn.py
#The requested operation requires elevation (Run as administrator).

#Error creating firewall rule: Command 'netsh advfirewall firewall add rule name="Allow_SQL_Server_Access" dir=in action=allow protocol=TCP localport=1433' returned non-zero exit status 1.
#mssql+pyodbc://DESKTOP-F54N72P\SQLEXPRESS/players?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes
#Successfully retrieved data from 'databases' table!

'''
   player_id          name
0          1  Naveen Kumar
1          2       Malathi
Successfully retrieved data from 'cricketers' table!
Connection to SQL Server closed.

Process finished with exit code 0

'''