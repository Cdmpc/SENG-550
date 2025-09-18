import psycopg2 as psql; # PostgresSQL Connector
from psycopg2.extras import execute_values; # Insert many rows with one query.
from psycopg2 import sql;
import numpy as np; # For array manipulation and fast matrix math if needed.
import pandas as pd;
import dotenv;
# ==================================================== [HELPER FUNCTIONS] =================================================================== #
def bulk_insert(table_name, df_arg, cursor_arg, conn_arg):
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

    # BULK INSERTION HAPPENS HERE, if the exact same thing already exists, do not add it
    # to the table.
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


def single_insert(table_name, row_dict, returning_col, cursor_arg, conn_arg):
    """
    Inserts ONE row into a given table.
    
    Args:
        table_name (str): Name of the table to insert into.
        row_dict (dict): A dictionary mapping column names -> values.
        returning_col (str): Column name whose value should be returned (e.g., primary key).
        cursor_arg (cursor): psycopg2 cursor object.
        conn_arg (connection): psycopg2 connection object.
    
    Returns:
        The value of the `returning_col` if successful,
        None if failed.
    """
    try:
        # Extract columns and values
        cols = list(row_dict.keys());
        vals = list(row_dict.values());

        # Build safe SQL identifiers using psycopg2.sql
        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders}) RETURNING {returning};").format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
            returning=sql.Identifier(returning_col)
        )

        # Execute query
        cursor_arg.execute(query, vals);
        result = cursor_arg.fetchone()[0];

        conn_arg.commit();
        print(f"INSERT INTO {table_name} SUCCESS! Returned {returning_col} = {result}");
        return result;

    except Exception as e:
        conn_arg.rollback();
        print(f"Error inserting into {table_name}: {e}");
        return None;

# ==================================================== [MAIN FUNCTION] =================================================================== #
def main():
    # STEP 1: Read the csv files into panda dataframes.
    cust_df = pd.read_csv(filepath_or_buffer="./A1/CSV/customers.csv");
    ord_df = pd.read_csv(filepath_or_buffer="./A1/CSV/orders.csv");
    del_df = pd.read_csv(filepath_or_buffer="./A1/CSV/deliveries.csv");

    # List of dataframes.
    dataframes_list = [cust_df, ord_df, del_df];

    want_to_delete = int(input("""NOTE: Highly recommend after a run or error to input (1), then run again with (0)
                               \nBulk Delete values in table after insertion? [1 for Yes, 0 for No]: """));
    print("You inputted:", want_to_delete);
    if(want_to_delete != 1 and want_to_delete != 0):
        print("Incorrect Response, please only use 1 or 0 to answer");
        return;

    PASSWORD = dotenv.get_key(dotenv_path="./.env", key_to_get="DB_PASSWORD"); # Stores the password safely away.
    
    # ========================== [STEP 2: DATABASE OPERATIONS] ================================== #
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

        # Drop the tables (ONLY ALLOWED IF THE TABLES ALREADY EXIST.)
        if(want_to_delete == 1):
            print("Using bulk delete...\n");
            bulk_delete(cursor_arg=psql_cursor, conn_arg=conn, table_name="deliveries", df_arg=del_df);
            bulk_delete(cursor_arg=psql_cursor, conn_arg=conn, table_name="orders", df_arg=ord_df);
            bulk_delete(cursor_arg=psql_cursor, conn_arg=conn, table_name="customers", df_arg=cust_df);
            return;
        
        # Insert bulk data into all 3 tables.
        cust_flag = 0; ord_flag = 0; del_flag = 0;
        
        cust_flag = bulk_insert(table_name="customers", df_arg=cust_df, cursor_arg=psql_cursor, conn_arg=conn);
        
        # Only insert into Orders, if inserting into Customers was successful.
        if(cust_flag == 1):
            ord_flag = bulk_insert(table_name="orders", df_arg=ord_df, cursor_arg=psql_cursor, conn_arg=conn);
        else:
            print("Customer Insertion failed, Orders table remains untouched...\n");
        
        # Only insert into Deliveries, if inserting into Orders was successful.
        if(ord_flag == 1):
            del_flag = bulk_insert(table_name="deliveries", df_arg=del_df, cursor_arg=psql_cursor, conn_arg=conn);
        else:
            print("Order Insertion failed, Delivieries table remains untouched...\n");
        
        if(del_flag == -1):
            print("Delivery Insertion failed...\n");
        else:
            print("Delivery Insertion SUCCESS!\n");
        

        # ============================== [STEP 3: ADD UPDATE DATA WITH PYTHON.] =========================
        # Adding customer Liam, and his order and delivery.
        psql_cursor.execute(
            query="""
            INSERT INTO customers (name, email, phone, address)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name, email, phone, address)
            DO NOTHING
            RETURNING customer_id
            """,
            vars=("Liam Nelson", "liam.nelson@example.com", "555-2468", "111 Elm Street")
        );
        customer_id = None;
        conn.commit();
        if psql_cursor.rowcount > 0:
            customer_id = psql_cursor.fetchone()[0];
            print(f"{psql_cursor.rowcount} row(s) updated successfully.");
        else:
            print("No customer rows were updated.");

        psql_cursor.execute(
            query="""
            INSERT INTO orders (customer_id, order_date, total_amount, product_id, product_category, product_name)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id, order_date, total_amount, product_id, product_category, product_name)
            DO NOTHING
            RETURNING order_id
            """,
            vars=(customer_id, "2025-06-01", "180.00", "116", "Electronics", "Bluetooth Speaker")
        );
        order_id = None;
        conn.commit();
        if psql_cursor.rowcount > 0:
            order_id = psql_cursor.fetchone()[0];
            print(f"{psql_cursor.rowcount} row(s) updated successfully.");
        else:
            print("No order rows were updated.");

        psql_cursor.execute(
            query="""
            INSERT INTO deliveries (order_id, delivery_date, status) 
            VALUES (%s, %s, %s)
            ON CONFLICT (order_id, delivery_date, status)
            DO NOTHING
            RETURNING delivery_id
            """,
            vars=(order_id, "2025-06-03", "Pending")
        );
        delivery_id = None;
        conn.commit();
        if psql_cursor.rowcount > 0:
            delivery_id = psql_cursor.fetchone()[0];
            print(f"{psql_cursor.rowcount} row(s) updated successfully.");
        else:
            print("No delivery rows were updated.");

        # Update Liam's delivery status to 'Shipped'
        psql_cursor.execute(
            query="""
            UPDATE deliveries
            SET status = %s
            WHERE delivery_id = %s
            """,
            vars=("Shipped", delivery_id)
        );
        conn.commit();

        # Adding one more customer, order and delivery.
        new_cust = {
            "name": "Carlos Morera Pinilla",
            "email": "carlos.morerapinilla@ucalgary.ca",
            "phone": "4031234567",
            "address": "1902 Starlight Blvd"
        }
        new_cust_id = single_insert(table_name="customers", row_dict=new_cust, returning_col="customer_id", cursor_arg=psql_cursor, conn_arg=conn);
        print("New customer id:", new_cust_id);

        new_ord = {
            "customer_id": new_cust_id,
            "order_date": "2025-09-17",
            "total_amount": 110.50,
            "product_id": 117,
            "product_category": "Appliances",
            "product_name": "Toaster Oven"
        }
        new_ord_id = single_insert(table_name="orders", row_dict=new_ord, returning_col="order_id", cursor_arg=psql_cursor, conn_arg=conn);
        print("New order id:", new_ord_id);

        new_del = {
            "order_id": new_ord_id,
            "delivery_date": "2025-09-21",
            "status": "Pending",
        }
        new_del_id = single_insert(table_name="deliveries", row_dict=new_del, returning_col="delivery_id", cursor_arg=psql_cursor, conn_arg=conn);
        print("New delivery id:", new_del_id);

        # Update delivery_id = 3, status to Delivered, on the deliveries table.
        # Update Liam's delivery status to 'Shipped'
        psql_cursor.execute(
            query="""
            UPDATE deliveries
            SET status = %s
            WHERE delivery_id = %s
            """,
            vars=("Delivered", 3)
        );
        conn.commit();

        
        # Close the cursor and database connection ALWAYS, when done.
        psql_cursor.close();
    
    conn.close();
    print("END OF PROGRAM...\n");
    return;


# =========================================================================================================================================================
if __name__ == "__main__":
    main();