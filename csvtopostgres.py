

import psycopg2
import csv
from datetime import datetime

# Set up the connection to the PostgreSQL database
#conn = psycopg2.connect(
#    host="localhost",
#    database="your_database",
#    user="your_username",
#    password="your_password"
#)

conn = psycopg2.connect(host="localhost", dbname="SqlAuto", user="postgres",
                        password="12345", port=5432)

cursor=conn.cursor()
#cursor.execute("SET datestyle = 'ISO, DMY'")
cursor.execute("SET datestyle = 'MDY'")

# Open the CSV file and read the data into a list of dictionaries
with open('Sales_Records_1M - Target.csv', 'r') as f:
    reader = csv.DictReader(f)
    print(reader.fieldnames)
    rows = []
    for row in reader:
        # Convert the date format to YYYY-MM-DD
        #if '%m-%d-%Y' in row['orderdate']:
        #   date1 = datetime.strptime(row['orderdate'], '%m-%d-%Y').strftime('%Y-%m-%d')
        #   row['orderdate'] = date1
        #elif '%m/%d/%Y' in row['orderdate']:
        #   date2 = datetime.strptime(row['orderdate'], '%m/%d/%Y').strftime('%Y-%m-%d')
        #   row['orderdate'] = date2

        try:
            date = datetime.strptime(row['orderdate'], '%m-%d-%Y').strftime('%Y-%m-%d')
            date2 = datetime.strptime(row['shipdate'], '%m-%d-%Y').strftime('%Y-%m-%d')
        except ValueError:
            date = datetime.strptime(row['orderdate'], '%m/%d/%Y').strftime('%Y-%m-%d')
            date2 = datetime.strptime(row['shipdate'], '%m/%d/%Y').strftime('%Y-%m-%d')
        row['orderdate'] = date
        row['shipdate'] = date2
        row['total_revenue'] = round(float(row['total_revenue']), 2)
        rows.append(row)

        # Insert the rows into the database
        #cursor.copy_from("""
        #    INSERT INTO sales1m (region, country, itemtype, sales_channel, orderpriority, orderdate,
        #                         order_id, shipdate, units_sold, unit_price, unitcost, total_revenue, totalcost,
        #                         total_profit)
        #    VALUES (%(region)s, %(country)s, %(itemtype)s, %(sales_channel)s, %(orderpriority)s,
        #            %(orderdate)s, %(order_id)s, %(shipdate)s, %(units_sold)s, %(unit_price)s,
        #            %(unitcost)s, %(total_revenue)s, %(totalcost)s, %(total_profit)s)
        #""", rows)

        # Use the COPY command to upload the data from the CSV file
        cursor.copy_from(f, 'sales1m_tgt', sep=',',
                         columns=('region', 'country', 'itemtype', 'sales_channel',
                                  'orderpriority', 'orderdate', 'order_id', 'shipdate', 'units_sold', 'unit_price',
                                  'unitcost', 'total_revenue', 'totalcost', 'total_profit'))



        # Convert the date format to YYYY-MM-DD
        #date = datetime.strptime(row['orderdate'], '%m-%d-%Y').strftime('%Y-%m-%d')
        #row['orderdate'] = date

# Insert the rows into the database
#cursor.executemany("""
#    INSERT INTO sales1m (region, country, itemtype, saleschannel, orderpriority, orderdate,
#                         orderid, shipdate, unitsold, unitprice, unitcost, totalrevenue, totalcost,
#                         totalprofit)
#    VALUES (%(Region)s, %(Country)s, %(Item Type)s, %(Sales Channel)s, %(Order Priority)s,
#            %(Order Date)s, %(Order ID)s, %(Ship Date)s, %(Units Sold)s, %(Unit Price)s,
#            %(Unit Cost)s, %(Total Revenue)s, %(Total Cost)s, %(Total Profit)s)
#""", rows)

# Set the datestyle setting in PostgreSQL
#with conn.cursor() as cursor:
#    cursor.execute("SET datestyle = 'ISO, DMY';")

# Open the CSV file and create a cursor to execute SQL queries

#with open('Sales_Records_1M.csv', 'r') as f:
#     cursor = conn.cursor()
#     # Skip the header row
#     next(f)
#     # Use the COPY command to upload the data from the CSV file
#     cursor.copy_from(f, 'sales1m', sep=',',
#     columns=('region','country','itemtype','sales_channel',
#     'orderpriority','orderdate','order_id','shipdate','units_sold','unit_price','unitcost','total_revenue','totalcost','total_profit'))



    # Commit the transaction
conn.commit()

    # Close the cursor and the database connection
cursor.close()
conn.close()
