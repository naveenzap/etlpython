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


#actual - select sum(tablecolumn) from sales
#target - select count(*) from salesaudit  where  tablename_col = salesaudit_sales

param_cols = {'tablecolumn_col': ["sum(sales) as totalcounts","sum(orderlinenumber)as totalcounts","sum(quantityordered)as totalcounts",
                                  "sum(priceeach) as totalcounts"],
'sumcolumns' : ["'salesaudit_sales'","'salesaudit_orderlinenumber'","'salesaudit_quantityordered'","'salesaudit_priceeach'"],
'console_tgt_counts': ["sales","orderlinenumber","quantityordered","priceeach"]}

tgt_totalcounts = 'totalcounts'

df  = pd.DataFrame(param_cols)

for index,row in df.iterrows():
    src_col = row['tablecolumn_col']
    tgt_col = row['sumcolumns']
    src_qry = f"SELECT {''.join(src_col)}  from sales"
    #print(src_qry)
    tgt_qry = f"SELECT {''.join(tgt_totalcounts)} from salesaudit where tablename_col = {tgt_col}"
    #print(tgt_qry)
    df1=pd.read_sql(src_qry,engine)
    df2=pd.read_sql(tgt_qry,engine)
    #Extract the columns as a String
    column_name_src = df1.columns[0]
    column_name_tgt = df2.columns[0]
    #Access the column values  by using the index operators[0]
    values_df1 = df1[column_name_src].iloc[0]
    values_df2 = df2[column_name_tgt].iloc[0]
    #print(values_df1)
    #print(values_df2)
    #Compare the column vales
    comparison_result = values_df1 == values_df2
    #print(comparison_result)

    false_values = comparison_result[comparison_result == False]
    #print(false_values)
    #print only false vales
    for value in false_values:
        if not value:
            print(f"{false_values} is present in {tgt_col} because of sales table:{values_df1} and targettable_columns-salesaudit-table:{values_df2}")