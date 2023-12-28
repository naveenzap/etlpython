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


start_query = time.time()
df = pd.read_sql_query("""
select * from sales
--limit 30
""",engine)

end_query = time.time()

start_time = time.time()

df.to_csv('sales.csv',index=False)

end_time = time.time()

export_time = end_time - start_time

print("file exported successfully")

print("file exported successfully in:", export_time ,"seconds.")
















