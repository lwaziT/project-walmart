import pandas as pd
import pymysql
from sqlalchemy import create_engine

import psycopg2

def extract():
    """ Extracts data from local csv file"""

    df = pd.read_csv('C:/Users/tobos/Desktop/Project-Walmart/walmart-10k-sales-datasets/Walmart.csv',
                     encoding_errors='ignore')
    return df

def data_quality_check(df):
    """ Performs data quality checks on data set """

    print(f'Size of dataframe: {df.shape}')
    """ Find duplicates """
    print(f'Number of duplicates in dataset: {df.duplicated().sum()}')

    """ Find null values """
    print(f'Number of null values in dataset: {df.isnull().sum()}')

def transform(df):
    """ Transform dataset: delete duplicate rows
                           delete rows with null values
                           convert column to float
                           add new column
    """
    # Delete duplicates
    df.drop_duplicates(inplace=True)

    # Delete rows with null values
    df.dropna(inplace=True)

    # Convert column "unit_price" to type float
    df['unit_price'] = df['unit_price'].str.replace('$', '').astype(float)

    # Create new column 'total'
    df['total'] = df['unit_price'] * df['quantity']
    return df

data = extract()
data_quality_check(data)

transformed_data = transform(data)
data_quality_check(transformed_data)

#MySQL connection
engine_mysql = create_engine("mysql+pymysql://root:root@localhost:3306/project_walmart_db")

try:
    engine_mysql
    print('Database connection successful')
except:
    print('Database connection failed')