import psycopg2 as psql; # PostgresSQL Connector
import numpy as np; # For array manipulation and fast matrix math if needed.
import pandas as pd; # For reading CSVs.

def establish_conn(db, user_arg):
    '''
    Establishes connection to Postgres DB Instance.
    '''
    conn = psql.connect(
        host = "a1seng550dbi.cjcwea4ieepr.us-west-2.rds.amazonaws.com",
        database="A1_SENG550_DBI",
        user="postgres",
        password=""
    );


def main():
    # Read the csv files into panda dataframes.
    cust_df = pd.read_csv(filepath_or_buffer="./CSVs/customers.csv");
    del_df = pd.read_csv(filepath_or_buffer="./CSVs/deliveries.csv");
    ord_df = pd.read_csv(filepath_or_buffer="./CSVs/orders.csv");

    establish_conn(db="A1_SENG550_DBI", user_arg="postgres");

    return;

if __name__ == "__main__":
    main();