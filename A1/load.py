import psycopg2 as ps_con; # PostgresSQL Connector
import numpy as np; # For array manipulation and fast matrix math if needed.
import pandas as pd; # For reading CSVs.

def main():
    # Read the csv files into panda dataframes.
    cust_df = pd.read_csv(filepath_or_buffer="./CSVs/customers.csv");
    del_df = pd.read_csv(filepath_or_buffer="./CSVs/orders.csv");

    return;

if __name__ == "__main__":
    main();