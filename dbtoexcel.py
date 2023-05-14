# cronscheduler

import psycopg2
import sqlalchemy
import pandas as pd
import csv
from sqlalchemy import create_engine
import datetime
from plyer import notification
import os

# conn = psycopg2.connect(host="localhost", dbname="SqlAuto", user="postgres",
#                        password="12345", port=5432)

# do sth
#
# cur.execute("""CREATE TABLE IF NOT EXISTS person (
# id INT PRIMARY KEY,
# name VARCHAR(255),
# age INT,
# gender CHAR);
# """)
# conn.commit() """


# cur.execute(""" SELECT * FROM person;
# """)
# print(cur.fetchone())

# cur.execute("""SELECT * from person WHERE age < 50;""")

# for row in cur.fetchall():
#    print(row)

# sql = cur.mogrify(""" SELECT * FROM person WHERE starts_with (name, %s) AND age < %s; """,("J",50))

# comm = cur.execute(""" SELECT * FROM person; """)

# comm = " SELECT * FROM person; "

# create a SQLAlchemy engine object


# db_string = psycopg2.connect(host="localhost", dbname="SqlAuto", user="postgres",
#                        password="12345", port=5432)

# change the working directory
os.chdir(r"C:\Users\DELL\OneDrive\Desktop\SqlAuto")

db_username = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = '5432'
db_name = 'SqlAuto'

db_string = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

# cur = conn.cursor()
# cur.execute(""" Insert INTO person (id,name,age,gender) VALUES
# (1,'Mike',30,'m'),
# (2,'Lisa',30,'f'),
# (3,'John',54,'m'),
# (4,'Bob',80,'m'),
# (5,'Julie',40,'f');
# """)

# db_string = psycopg2.connect("postgresql://localhost:12345@localhost:5432/SqlAuto")

engine = create_engine(db_string)
try:
    # Export the first data set
    df1 = pd.read_sql(""" SELECT * FROM sales1m; """, engine)
    print(df1)
    # Export data to excel
    df1.to_csv("data1_" + datetime.datetime.now().strftime('%d-%b-%Y %H%M%S') + ".csv", index=False)

    # Export the second data set
    df2 = pd.read_sql(""" SELECT * FROM department; """, engine)
    print(df2)
    df2.to_csv("data2_" + datetime.datetime.now().strftime('%d-%b-%Y %H%M%S') + ".csv", index=False)

except Exception as e:
    # send Notification to user
    # print(f"Error occured: {}".format(e))
    print(f"Error occurred: {e}")
    # notification.notify(title="script Status",
    #message = f'Data has been successfully exported into Excel.\nTotal Rows: {df1.shape[0]} \n Total Columns: {df1.shape[1]}',
    # message=f"Error occurred: {e}",timeout=10)

# cur.execute(cur)

# print(df.fetchall())

# tables= df.fetchall()


# df.close()
# conn.commit()

# conn.close()

# print('Data exported successfully to', file_path)

# df = pd.read_sql_query(cur, conn)


# cur.close()


# conn.close()
