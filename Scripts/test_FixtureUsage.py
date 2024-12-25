import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify
from Config.config import *
import pytest
import logging
# Logging mechanism
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to app end, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

@pytest.mark.smoke
@pytest.mark.usefixtures("setup","display")
class TestExample:
    def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL(self,setup):
        #engine_list = setup
        mysql_engine = setup[0]
        oracle_engine = setup[1]
        logger.info(" Data extraction from sales_data.csv to sales_staging has started .......")
        try:
            file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', mysql_engine)
            logger.info(" Data extraction from sales_data.csv to sales_staging has completed .......")
        except Exception as e:
            logger.error(f"Error occured during data extraction: {e}")
            pytest.fail(f"Test failed due to an error {e}")
