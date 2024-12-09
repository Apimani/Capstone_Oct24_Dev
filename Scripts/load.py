import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

# Create mysql engine
#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')
from Scripts.config import *

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
#oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

def  load_fact_sales():
    query =  """insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date)
                 select sales_id,product_id,store_id,quantity,total_amount,sale_date from sales_with_deatils;"""
    conn = mysql_engine.connect()
    conn.execute(query)
    conn.commit()


if __name__== '__main__':
    print("Data load strted ....")
    load_fact_sales()
    print("Data load completed ....")

