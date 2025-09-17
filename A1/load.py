import psycopg2 as psql; # PostgresSQL Connector
from psycopg2.extras import execute_values; # Insert many rows with one query.
import numpy as np; # For array manipulation and fast matrix math if needed.
import pandas as pd;
import dotenv;
# ==================================================== [HELPER FUNCTIONS] =================================================================== #
def insert_bulk(table_name, df_arg, cursor_arg, conn_arg):
    '''
    Inserts many rows at the same time into a particular table.
    It also forces a constraint to make the combination of the rest of the attributes
    except the primary key UNIQUE, so that during development the table does not quickly
    get super large.
    Returns a flag, 1 if successful, -1 is failed.
    '''
    df_arg = pd.DataFrame(df_arg);
    # Convert DataFrame to list of TUPLES.
    records = [tuple(x) for x in df_arg.to_numpy()];

    # The column names from the dataframe, as one string with , delimiters.
    cols = ",".join(list(df_arg.columns));

    # Enforces all attributes except the PK are not duplicated in the table.
    try:
        cursor_arg.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_unique_index
            ON {table_name} ({cols})
            """);
        print(f"Unique constraint {table_name}_unique_index was succesfully added!");
    except Exception as e:
        conn_arg.rollback();
        print(f"Error adding unique constraint to {table_name}: {e}")
        return -1;

    # BULK INSERTION HAPPENS HERE.
    query = f"""
    INSERT INTO {table_name} ({cols}) 
    VALUES %s
    ON CONFLICT ({cols}) 
    DO NOTHING
    """;
    try:
        execute_values(cur=cursor_arg, sql=query, argslist=records);
        
        # commit() places the data to the postgresSQL database.
        conn_arg.commit();
        print(f"BULK INSERT OPERATION INTO {table_name} SUCCESS!\n");
        return 1;
    except Exception as e:
        # Undoes all transactions that were part of the query that failed.
        conn_arg.rollback();
        print(f"Error inserting into {table_name}: {e}");
        return -1;


def bulk_delete(table_name, df_arg, conn_arg, cursor_arg):
    '''
    Deletes every entry from each table, but DOES NOT delete the table itself.
    '''
    df_arg = pd.DataFrame(df_arg);

    # Required to be a list of tuples for execute_values.
    records = [tuple(x) for x in df_arg.to_numpy()];

    # Truncate the table to reset the PKs when the data is deleted from a table.
    query = f"""TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE""";

    try:
        cursor_arg.execute(query= query);
        conn_arg.commit();
        print(f"BULK DELETE OPERATION FROM {table_name} WAS SUCCESSFUL!");
        return 1;

    except Exception as e:
        conn_arg.rollback();
        print(f"Error bulk deleting {table_name}: {e}");
        return -1;



# ==================================================== [MAIN FUNCTION] =================================================================== #

def main():
    # Read the csv files into panda dataframes.
    cust_df = pd.read_csv(filepath_or_buffer="./A1/CSV/customers.csv");
    ord_df = pd.read_csv(filepath_or_buffer="./A1/CSV/orders.csv");
    del_df = pd.read_csv(filepath_or_buffer="./A1/CSV/deliveries.csv");

    # List of dataframes.
    dataframes_list = [cust_df, ord_df, del_df];

    want_to_delete = int(input("Bulk Delete values in table after insertion? [1 for Yes, 0 for No]: "));
    print("You inputted:", want_to_delete);
    if(want_to_delete != 1 and want_to_delete != 0):
        print("Incorrect Response, please only use 1 or 0 to answer");
        return;

    PASSWORD = dotenv.get_key(dotenv_path="./.env", key_to_get="DB_PASSWORD"); # Stores the password safely away.
    
    # ========================== [DATABASE OPERATIONS] ================================== #
    conn = psql.connect(
        host = "a1seng550dbi.cjcwea4ieepr.us-west-2.rds.amazonaws.com", # NOTE: Could change to input so I can swap between AWS and my local server.
        database="A1_SENG550_DBI",
        user="postgres",
        password=PASSWORD
    );

    # Check if connection succeeded, if not close it.
    if(conn.closed != 0):
        print("Connection to database failed, something went wrong...");
        conn.rollback();
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
        
        if(del_flag == -1):
            print("Delivery Insertion failed...\n");
        else:
            print("Delivery Insertion SUCCESS!\n");
        

        # Drop the tables (ONLY ALLOWED IF THE TABLES ALREADY EXIST.)
        if(want_to_delete == 1):
            print("Using bulk delete...\n");
            bulk_delete(cursor_arg=psql_cursor, conn_arg=conn, table_name="deliveries", df_arg=del_df);
            bulk_delete(cursor_arg=psql_cursor, conn_arg=conn, table_name="orders", df_arg=ord_df);
            bulk_delete(cursor_arg=psql_cursor, conn_arg=conn, table_name="customers", df_arg=cust_df);

        # Close the cursor and database connection ALWAYS, when done.
        psql_cursor.close();
    
    conn.close();
    return;

if __name__ == "__main__":
    main();