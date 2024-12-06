import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

# Create mysql engine
mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')

# Create Oracle engine
oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')

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


if __name__== '__main__':
    print("Data Extrcation strted ....")
    extract_sales_dataSRC_Load_STG()
    extract_product_dataSRC_Load_STG()
    extract_inventory_dataSRC_Load_STG()
    extract_supplier_dataSRC_Load_STG()
    print("Data Extrcation completed ....")

