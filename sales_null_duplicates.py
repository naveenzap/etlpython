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

column_name_dup = "count(*)"
column_name_null = "*"


sales = {
    'country': ["'USA'","'Italy'","'Ireland'","'Australia'"],
    'territory' : ["'NA'","'EMEA'","'EMEA'","'APAC'"]
}

df = pd.DataFrame(sales)
result_duplicates = pd.DataFrame()
result_duplicates_final = pd.DataFrame()

#duplicate query


for index,row in df.iterrows():
    dup_coun = row['country']
    dup_terr = row['territory']
    dup_qry = f"SELECT {''.join(column_name_dup)} from sales where country = {dup_coun} and territory = {dup_terr} group by country having count(*) > 1"
    result_dup = pd.read_sql(dup_qry,engine)
    result_dataframes = []
    result_dataframes.append(df)
    if result_dup.empty:
        print(f"No records found in this country and region for duplicates check")
    else:
        print(result_dup)
    print(type(dup_qry))
    dup_qry_conversion = pd.DataFrame([row.split(",") for row in dup_qry.splitlines()])
    print(dup_qry_conversion)
    dup_query_df = pd.DataFrame(dup_qry_conversion)
    #extract_dup = pd.concat(dup_query_df)
    result_duplicates = pd.concat([result_duplicates, dup_query_df])
    result_duplicates_final = pd.concat([result_duplicates, result_dup])
    #result_duplicates = pd.concat([result_duplicates,result_dup])
    #result_duplicates_final.to_excel('sales_duplicates_country_territory_wise.xlsx',engine='xlsxwriter')
    result_duplicates_final.to_csv('sales_duplicates_country_territory_wise.csv')

#Null query

    for index, row in df.iterrows():
        dup_coun = row['country']
        dup_terr = row['territory']
        dup_qry = f"SELECT {''.join(column_name_null)} from sales where country = {dup_coun} and territory = {dup_terr} and country is null or territory is null"
        result_dup = pd.read_sql(dup_qry, engine)
        result_dataframes = []
        result_dataframes.append(df)
        if result_dup.empty:
            print(f"No records found in this country and region for Null check")
        else:
            print(result_dup)
        print(type(dup_qry))
        dup_qry_conversion = pd.DataFrame([row.split(",") for row in dup_qry.splitlines()])
        print(dup_qry_conversion)
        dup_query_df = pd.DataFrame(dup_qry_conversion)
        # extract_dup = pd.concat(dup_query_df)
        result_duplicates = pd.concat([result_duplicates, dup_query_df])
        result_duplicates_final = pd.concat([result_duplicates, result_dup])
        # result_duplicates = pd.concat([result_duplicates,result_dup])
        # result_duplicates_final.to_excel('sales_duplicates_country_territory_wise.xlsx',engine='xlsxwriter')
        result_duplicates_final.to_csv('sales_null_country_territory_wise.csv')

    '''
    import pandas as pd

    # Check if dup_qry is a string containing comma-separated values
    if "," in dup_qry:
        # Split string into list of values
        values_list = dup_qry.split(",")

        # Create DataFrame from the list
        dup_df = pd.DataFrame(values_list, columns=["Column1"])  # Assign a column name

        # Use dup_df in concatenation
        result_duplicates = pd.concat([result_duplicates, dup_df, result_dup])

    else:
        # Handle the case where dup_qry is not a comma-separated string
        print(
            "dup_qry is not a comma-separated string. Please provide its format or intended use for further assistance.")
    '''