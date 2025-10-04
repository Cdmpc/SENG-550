import numpy as np;
import pandas as pd;
import psycopg2 as psql;
from psycopg2.extras import execute_values; # Insert many values with one statement.
from psycopg2 import sql;
import dotenv;

# ======================================================================== [FUNCTION] ======================================================================= #
def bulk_delete(table_name, conn_arg, cursor_arg):
    '''
    Deletes every entry from each table, but DOES NOT delete the table itself.
    ## Returns:
    - An integer flag, 1 if operation was successful, -1 if operation failed.
    '''
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

def get_most_recent_customer(customer_id, cursor_arg, conn_arg):
    # Store the entire tuple of the data, this is what is returned.
    try:
        cursor_arg.execute(
            query=f"""
            SELECT *
            FROM dim_customers
            WHERE customer_id = {customer_id}    
            """
        );
        all_customers = cursor_arg.fetchall();
        conn_arg.commit();
        return all_customers[len(all_customers) - 1];
    except Exception as e:
        conn_arg.rollback();
        print("get_most_recent_customer failed!\n", e);

def get_most_recent_product(product_id, cursor_arg, conn_arg):
    try:
        cursor_arg.execute(
            query=f"""
            SELECT *
            FROM dim_products
            WHERE product_id = {product_id}    
            """
        );
        all_products = cursor_arg.fetchall();
        conn_arg.commit();
        return all_products[len(all_products) - 1];
    except Exception as e:
        conn_arg.rollback();
        print("get_most_recent_product failed!\n", e);

def add_customer(customer_id, name, email, city, conn_arg, cursor_arg):
    '''
    Inserts a new customer into the dim_customers table given the non-dimensional values.
    ON CONFLICT Checks mean that the exact same non-dimensional data cannot be inserted.

    ## Returns:
        Tuple representation of the added data.
    '''
    try:
        cursor_arg.execute(
        query=f"""
                INSERT INTO dim_customers (customer_id, name, email, city)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (customer_id, name, email, city) 
                DO NOTHING
                RETURNING id
                """,
                vars=(customer_id, name, email, city)
        );
        customer_result = cursor_arg.fetchone();

        # Required to commit and show the updated data in the pgadmin GUI.
        conn_arg.commit();
        print(f"Dim_customer insertion of {customer_id}, {name} was successful!\n");
        return customer_result;

    except Exception as e:
        # Forces ACID compliance, by removing the query back to the last successful query.
        conn_arg.rollback();
        print(f"Dim_customer insertion of {customer_id}, {name} failed!\n", e);
        return None;

def add_product(product_id, name, category, price, conn_arg, cursor_arg):
    '''
    Inserts a new product into the dim_products table given the non-dimensional values.
    ON CONFLICT Checks mean that the exact same non-dimensional data cannot be inserted.

    ## Returns:
        Tuple representation of the newly inserted data.
    '''
    try:
        cursor_arg.execute(
            query=f"""
                INSERT INTO dim_products (product_id, name, category, price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id, name, category, price) 
                DO NOTHING
                RETURNING id
                """,
                vars=(product_id, name, category, price)
        );
        product_result = cursor_arg.fetchone();

        # Required to commit and show the updated data in the pgadmin GUI.
        conn_arg.commit();
        print(f"Dim_product insertion of {product_id}, {name} was successful!\n");
        return product_result;

    except Exception as e:
        conn_arg.rollback();
        print(f"Dim_product insertion of {product_id}, {name} failed!\n", e);
        return None;

def add_order(product_id, customer_id, amount, conn_arg, cursor_arg):
    '''
    Inserts a new product into the fact_orders table given the non-dimensional values.
    ON CONFLICT ensures the exact same order of data does not appear in the table.

    ## Returns:
        Tuple representation of the newly added data.
    '''
    try:
        cursor_arg.execute(
            query=f"""
                INSERT INTO fact_orders (product_id, customer_id, amount)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id, customer_id, amount) 
                DO NOTHING
                RETURNING order_id
                """,
                vars=(product_id, customer_id, amount)
        );
        order_result = cursor_arg.fetchone();

        # Required to commit and show the updated data in the pgadmin GUI.
        conn_arg.commit();
        print(f"Order was added succesfully!\n");
        return order_result;

    except Exception as e:
        conn_arg.rollback();
        print(f"Order insertion failed!\n", e);
        return None;

def update_customer_city(customer_id, name, email, new_city, conn_arg, cursor_arg):
    '''
    Updates the city field of the customer dimensional table, by adding a new column where the City column is changed,
    identified by the new surrogate key. The end date of the old column will be filled, and this new entry will have a NULL
    for end_date.

    ## Returns:
    - Tuple representation of the new data. Or None if failed.
    '''
    most_recent_customer = get_most_recent_customer(customer_id, cursor_arg, conn_arg);
    try:
        # Retire the old column with the old city. The id would be stored in 0th index of the tuple from the above function.
        cursor_arg.execute(
            query=f"""
            UPDATE dim_customers
            SET valid_end_date = CURRENT_TIMESTAMP
            WHERE id = {most_recent_customer[0]}
            """
        )
        
        # Insert the new column, as a new entry into the dim_customer table.
        new_customer_result = add_customer(customer_id, name, email, new_city, conn_arg, cursor_arg);
        conn_arg.commit();
        print(f"Updating Customer city succeeded, new row added with new dates and city!\n");
        return new_customer_result;
    except Exception as e:
        conn_arg.rollback();
        print(f"Updating customer city failed!\n", e);
        return None;

def update_product_price(product_id, name, category, new_price, conn_arg, cursor_arg):
    '''
    Updates the product price of the dim_products table, using the same method as the update_customer_city() method.

    ## Returns:
    - Tuple representation of the new data. None if failed.
    '''
    most_recent_product = get_most_recent_product(product_id, cursor_arg, conn_arg);
    try:
        # Retire the old product price.
        cursor_arg.execute(
            query=f"""
            UPDATE dim_products
            SET valid_end_date = CURRENT_TIMESTAMP
            WHERE id = {most_recent_product[0]}
            """
        );
        new_product_result = add_product(product_id, name, category, new_price, conn_arg, cursor_arg);
        conn_arg.commit();
        print(f"Updating product price succeeded, new row added with new dates and price!\n");
        return new_product_result;

    except Exception as e:
        conn_arg.rollback();
        print(f"Updating product price failed!\n", e);
        return None;
        

# ======================================================================== [MAIN] ======================================================================= #
def main():
    # User input to start on fresh tables when starting the program.
    want_to_delete = int(input("""Do you want to delete the data for the tables and start on fresh empty tables? (Recommended to run 1 - Yes first)\n
1 - Yes\n2 - No\nType the corresponding number (only the number, no spaces!):"""));
    
    while(want_to_delete != 1 and want_to_delete != 2):
        print("Invalid input...\n");
        want_to_delete = int(input("""Do you want to delete the data for the tables and start on fresh empty tables? (Recommended to run 1 - Yes first)\n
1 - Yes\n2 - No\nType the corresponding number (only the number, no spaces!):"""));
    
    # Grab secret values (not shown for security purposes.)
    DB_ENDPOINT = str(dotenv.get_key(dotenv_path="./.env", key_to_get="DB_ENDPOINT"));
    DB_PASSWORD = str(dotenv.get_key(dotenv_path="./.env", key_to_get="DB_PASSWORD"));

    # Use the PostgresSQL Database name from pgadmin4, not the DB identifier on Amazon RDS.
    DATABASE_NAME = str("seng550_a2_dbi");

    # Use credentials and endpoint to connect to the database server.
    conn = psql.connect(
        host=DB_ENDPOINT,
        user="postgres",
        password=DB_PASSWORD,
        database=DATABASE_NAME
    );
    cursor = conn.cursor();

    # Bulk deletion of tables to start fresh.
    if(want_to_delete == 1):
        print("Using bulk delete...\n");
        bulk_delete(table_name="dim_customers", conn_arg=conn, cursor_arg=cursor);
        bulk_delete(table_name="dim_products", conn_arg=conn, cursor_arg=cursor);
        bulk_delete(table_name="fact_orders", conn_arg=conn, cursor_arg=cursor);
        print("Bulk delete completed, program finished...");
        return;

    # SQL Connection to database failed
    if(conn.closed != 0):
        print("Connection to database failed, something went wrong...\n");
        conn.rollback();
        conn.close();
        return -1;
    
    # SQL Connection to database succeeded
    else:
        print(f"Connection to {DATABASE_NAME} was successful!\n");

    # This portion of the program, only executes if connection to database was successful.
    # Create a cursor to interact with the database.
    customer = tuple(); product = tuple(); order = tuple();

    # ============================== BEGIN QUERIES ======================================== :
    # 1. Add product P1 (Laptop, Electronics, $1000)
    add_product(product_id=1, name="Laptop", category="Electronics", price="$1000.00", conn_arg=conn, cursor_arg=cursor);

    # 2. Add product P2 (Phone, Electronics, $500)
    add_product(product_id=2, name="Phone", category="Electronics", price="$500.00", conn_arg=conn, cursor_arg=cursor);

    # 3. Add customer C1 (Alice, New York)
    add_customer(customer_id=1, name="Alice", email="", city="New York", conn_arg=conn, cursor_arg=cursor);

    # 4. Add customer C2 (Bob, Boston)
    add_customer(customer_id=2, name="Bob", email="", city="Boston", conn_arg=conn, cursor_arg=cursor);

    # 5. Add order O1: C1 buys P1 for $1000
    add_order(product_id=1, customer_id=1, amount="$1000", conn_arg=conn, cursor_arg=cursor);

    # 6. Update C1’s city to Chicago
    update_customer_city(customer_id=1, name=get_most_recent_customer(1, cursor, conn)[2], 
                         email="", new_city="Chicago", conn_arg=conn, cursor_arg=cursor);
    
    # 7. Update P1’s price to $900
    update_product_price(product_id=1, name=get_most_recent_product(1, cursor, conn)[2], category=get_most_recent_product(1, cursor, conn)[3], 
                         new_price="$900.00", conn_arg=conn, cursor_arg=cursor);
    
    # 8. Add order O2: C1 buys P1 for $850
    add_order(product_id=1, customer_id=1, amount="$850.00", conn_arg=conn, cursor_arg=cursor);

    # 9. Update C2’s city to Calgary
    update_customer_city(customer_id=2, name=get_most_recent_customer(2, cursor, conn)[2], 
                         email="", new_city="Calgary", conn_arg=conn, cursor_arg=cursor);
    
    # 10. Add order O3: C2 buys P2 for $500
    add_order(product_id=2, customer_id=2, amount="$500.00", conn_arg=conn, cursor_arg=cursor);

    # 11. Add order O4: C1 buys P1 for $900
    add_order(product_id=1, customer_id=1, amount="$900.00", conn_arg=conn, cursor_arg=cursor);

    # 12. Update C1’s city to San Francisco
    update_customer_city(customer_id=1, name=get_most_recent_customer(1, cursor, conn)[2], 
                         email="", new_city="San Francisco", conn_arg=conn, cursor_arg=cursor);

    # 13. Add order O5: C1 buys P2 for $450
    add_order(product_id=2, customer_id=1, amount="$450.00", conn_arg=conn, cursor_arg=cursor);

    # 14. Add order O6: C2 buys P1 for $900
    add_order(product_id=2, customer_id=1, amount="$900.00", conn_arg=conn, cursor_arg=cursor);


if __name__ == "__main__":
    main();