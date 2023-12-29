import pandas as pd
import psycopg2
import urllib.parse
import time
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy import select

password_file_path = r'C:\Users\DELL.DESKTOP-0EDAUKR\Desktop\postgres_password\password.txt'

with open(password_file_path, 'r') as file:
    password = file.read().strip()

#encode the passsword
encoded_password = urllib.parse.quote_plus(password)

# Replace the following with your actual database details
dbname = "sqlauto"
user = "postgres"
password = "12345"
host = "localhost"
port = 5432

# Connect to the database
#conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
)

#conn
#conn = f"postgresql://username:{encoded_password}@host:5432/dbname"

print("Successfully connected to the database")


columns = "count(*)"
table_parameterized = { 'table': ["sales","salesaudit"]}

df_tab = pd.DataFrame(table_parameterized)

all_results = pd.DataFrame()

#start the time for table executions

for index,row in df_tab.iterrows():
    tables = row['table']
    query1 = f"SELECT {''.join(columns)},'{''.join({tables})}' as table_nm from {tables}"
    print(query1)
    count = pd.read_sql(query1,engine)
    if count.empty:
        print(f"No records found in {tables}")
    else:
        pass
    all_results = pd.concat([all_results,count],ignore_index=True)
    all_results.to_excel('table_export.xlsx')