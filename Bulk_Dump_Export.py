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

#query_name,query,output_file
query_list = [
    ("sales_whole_data",
    "select * from sales",
    "sales_whole_data"),
    ("sales_audit_data",
    "select * from salesaudit",
     "sales_audit_data")
]

for query_name,query,output_file in query_list:
    print(query)
    #start time of pandas query
    start_query = time.time()
    df = pd.read_sql_query(query,engine)
    end_query = time.time()
    query_time = end_query - start_query
    #start the timer
    start_time = time.time()
    output_file = f"{query_name}_output.xlsx"
    df.to_excel(output_file)
    #calculate the time taken
    end_time = time.time()
    execution_time = end_time - start_time
    print("query running time: ",query_time,"seconds")
    print("file exported successfully in: ",execution_time,"seconds.")