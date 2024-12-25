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
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)


@pytest.fixture()
def setup():
    # Create mysql engine
    mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
    # Create Oracle engine
    oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')
    logger.info("Database connection is succesfully established")
    yield [mysql_engine,oracle_engine]
    mysql_engine.dispose()
    oracle_engine.dispose()
    logger.info("Database connection is closed successfully")

@pytest.fixture()
def display():
    # Create mysql engine
    print("display fixture started ...")
    yield
    print("display fixture finished ...")
