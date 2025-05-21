import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def extract():
    """ Extracts data from local csv file"""
    filepath = 'C:/Users/tobos/Desktop/Project-Walmart/walmart-10k-sales-datasets/Walmart.csv'

    try:
        df = pd.read_csv(filepath,encoding_errors='ignore')
        logging.info('Successfully extracted data from local csv file')
        return df
    except Exception as e:
        logging.error(f'Failed to read data from local csv file: {e}')
        raise

def data_quality_checks(df):
    """ Performs data quality checks on data set """
    logging.info(f"DataFrame shape: {df.shape}")
    logging.info(f"Duplicate rows: {df.duplicated().sum()}")
    logging.info("Null values per column:")
    logging.info(f"\n{df.isnull().sum()}")

def transform(df):
    """ Transform dataset:
                    Removes duplicate rows
                    Drops rows with null values
                    Converts 'unit_price' column to float
                    Adds 'total' column
    """
    # Delete duplicates
    df.drop_duplicates(inplace=True)

    # Delete rows with null values
    df.dropna(inplace=True)

    # Convert column "unit_price" to type float
    try:
        df['unit_price'] = df['unit_price'].str.replace('$', '').astype(float)
    except Exception as e:
        logging.error(f'Failed to convert data into float: {e}')
        raise

    # Create new column 'total'
    df['total'] = df['unit_price'] * df['quantity']
    logging.info("Transformation complete.")
    return df

def load_to_mysql(df, table_name, db_url):
    """ Loads DataFrame to MySQL database """
    try:
        engine = create_engine(db_url)
        engine.connect()
        logging.info("Database connection established successfully.")

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info('Successfully loaded data into MySQL database.')

    except Exception as e:
        logging.error(f'Failed to load data into MySQL database: {e}')
        raise

if __name__ == "__main__":

    # Extract from csv file
    data = extract()

    # Perform data quality checks
    data_quality_checks(data)

    # Transform data and store in variable
    transformed_data = transform(data)

    # Perform data quality checks on transformed data
    data_quality_checks(transformed_data)

    # Establish MySQL connection
    mysql_url = "mysql+pymysql://root:root@localhost:3306/project_walmart_db"

    # Load to MySQL
    load_to_mysql(transformed_data, table_name='project_walmart_db', db_url=mysql_url)