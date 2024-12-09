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

def extract_sales_dataSRC_Load_STG():
    df = pd.read_csv("data/sales_data.csv")
    df.to_sql("staging_sales",mysql_engine,if_exists='replace',index=False)

def extract_product_dataSRC_Load_STG():
    df = pd.read_csv("data/product_data.csv")
    df.to_sql("staging_product",mysql_engine,if_exists='replace',index=False)


def extract_supplier_dataSRC_Load_STG():
    df = pd.read_json("data/supplier_data.json")
    df.to_sql("staging_supplier",mysql_engine,if_exists='replace',index=False)

def extract_inventory_dataSRC_Load_STG():
    df = pd.read_xml("data/inventory_data.xml",xpath=".//item")
    df.to_sql("staging_inventory",mysql_engine,if_exists='replace',index=False)

def extract_stores_data_OracleSRC_Load_STG():
    query =  """select * from stores"""
    df = pd.read_sql(query,oracle_engine)
    df.to_sql("staging_stores",mysql_engine,if_exists='replace',index=False)


if __name__== '__main__':
    print("Data Extrcation strted ....")
    extract_sales_dataSRC_Load_STG()
    extract_product_dataSRC_Load_STG()
    extract_inventory_dataSRC_Load_STG()
    extract_supplier_dataSRC_Load_STG()
    extract_stores_data_OracleSRC_Load_STG()
    print("Data Extrcation completed ....")

