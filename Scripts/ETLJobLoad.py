from Scripts.extract import *
from Scripts.transform import *
from Scripts.load import *
import logging
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)



if __name__== '__main__':
    print("Data Extrcation strted ....")
    extract_sales_dataSRC_Load_STG()
    extract_product_dataSRC_Load_STG()
    extract_inventory_dataSRC_Load_STG()
    extract_supplier_dataSRC_Load_STG()
    extract_stores_data_OracleSRC_Load_STG()
    print("Data Extrcation completed ....")
    print("Data transformation strted ....")
    filter_sales_data()
    router_sales_data()
    aggregate_sales_data()
    join_sales_data()
    aggregate_inventory_levels()
    print("Data transformation completed ....")
    logger.info("Data load strted ....")
    load_fact_sales()
    load_inventory_fact()
    load_inventory_levels_by_store()
    load_Monththly_summary()
    logger.info("Data load finsihed ....")


