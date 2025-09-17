import psycopg2 as psql; # PostgresSQL Connector
from psycopg2.extras import execute_values; # Insert many rows with one query.
import numpy as np; # For array manipulation and fast matrix math if needed.
import pandas as pd;
import dotenv;
# ==================================================== [HELPER FUNCTIONS] =================================================================== #
def insert_bulk(table_name, df_arg, cursor_arg, conn_arg):
    '''
    Inserts many rows at the same time into a particular table.
    Returns a flag, 1 if successful, -1 is failed.
    '''
    df_arg = pd.DataFrame(df_arg);
    # Convert DataFrame to list of TUPLES.
    records = [tuple(x) for x in df_arg.to_numpy()];

    # The column names from the dataframe, as one string with , delimiters.
    cols = ",".join(list(df_arg.columns));

    insert_into_query = f"INSERT INTO {table_name} ({cols}) VALUES %s";
    try:
        execute_values(cur=cursor_arg, sql=insert_into_query, argslist=records);

        # commit() places the data to the postgresSQL database.
        conn_arg.commit();
        print(f"Success, inserted {len(records)} records into {table_name}");
        return 1;
    except Exception as e:
        conn_arg.rollback();
        print(f"Error inserting into {table_name}: {e}");
        return -1;

# ==================================================== [MAIN FUNCTION] =================================================================== #
def main():
    # Read the csv files into panda dataframes.
    cust_df = pd.read_csv(filepath_or_buffer="./CSVs/customers.csv");
    ord_df = pd.read_csv(filepath_or_buffer="./CSVs/orders.csv");
    del_df = pd.read_csv(filepath_or_buffer="./CSVs/deliveries.csv");

    # List of dataframes.
    dataframes_list = [cust_df, ord_df, del_df];

    PASSWORD = dotenv.get_key(dotenv_path="../.env", key_to_get="DB_PASSWORD"); # Stores the password safely away.
    
    # ========================== [DATABASE OPERATIONS] ================================== #
    conn = psql.connect(
        host = "a1seng550dbi.cjcwea4ieepr.us-west-2.rds.amazonaws.com",
        database="A1_SENG550_DBI",
        user="postgres",
        password=PASSWORD
    );

    # Check if connection succeeded, if not close it.
    if(conn.closed != 0):
        print("Connection to database failed, something went wrong...");
        conn.close();
    
    else:
        print("Connection to database was successful!\n");
        # Create a cursor to point to the SQL Database in order to make queries to it.
        psql_cursor = conn.cursor();

        # Insert bulk data into all 3 tables.
        cust_flag = 0; ord_flag = 0; del_flag = 0;
        
        cust_flag = insert_bulk(table_name="customers", df_arg=cust_df, cursor_arg=psql_cursor, conn_arg=conn);
        # Only insert into Orders, if inserting into Customers was successful.
        if(cust_flag == 1):
            ord_flag = insert_bulk(table_name="orders", df_arg=ord_df, cursor_arg=psql_cursor, conn_arg=conn);
        else:
            print("Customer Insertion failed...\n");
        # Only insert into Deliveries, if inserting into Orders was successful.
        if(ord_flag == 1):
            del_flag = insert_bulk(table_name="deliveries", df_arg=del_df, cursor_arg=psql_cursor, conn_arg=conn);
        else:
            print("Order Insertion failed...\n");

        # Close the cursor and database connection ALWAYS, when done.
        psql_cursor.close();
    
    conn.close();
    return;

if __name__ == "__main__":
    main();