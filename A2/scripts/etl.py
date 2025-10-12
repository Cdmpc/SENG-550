import numpy as np; import pandas as pd; import psycopg2 as psql;
import dotenv; import part2;

# MongoDB Driver Connectors
from pymongo.mongo_client import MongoClient;
from pymongo.server_api import ServerApi;

# Pandas SQL reads a SQLAlchemy connection string. (pip install SQLAlchemy)
from sqlalchemy import create_engine;

# MongoDB Connection string, URI is stored in environment variable.
uri = str(dotenv.get_key(dotenv_path= "./.env", key_to_get="MONGO_URI"));

def main():
    # NOTE: YOU MUST HAVE THE POSTGRESQL DATABASE POPULATED UP TO THE PART 3 QUERIES TO USE
    # THIS SCRIPT, if not, run part2.py FIRST and run the Part3.SQL command
    # on the Postgres server!

    # Attempt to connect to MongoDB Server.
    mongo_cli = MongoClient(uri, server_api=ServerApi('1'));
    try:
        # Ping the database server.
        mongo_cli.admin.command('ping');
        print("Ping successful, MongoDB instance is accessible!");
    except Exception as e:
        print("Connecting to the Mongo Client failed because:\n", e);
        return;

    # Bulk delete of MongoDB document.
    bulk_delete = int(input("Bulk delete MongoDB collection?: (0 - No, 1 - Yes): "));
    if(bulk_delete != 0 and bulk_delete != 1):
        print("Invalid input, please only enter 0 or 1, no spaces!");
        return;

    # Get the column names from the MongoDB client.
    sales_db = mongo_cli.get_database("sales_db");
    orders_summary = sales_db.get_collection("orders_summary");

    if(bulk_delete == 1):
        res = orders_summary.delete_many({});
        print(f"{res.deleted_count} documents deleted from '{orders_summary.name}'.")
        mongo_cli.close();
        return;

    # Connect to PostgreSQL.
    # Grab secret values (not shown for security purposes.)
    DB_ENDPOINT = str(dotenv.get_key(dotenv_path="./.env", key_to_get="DB_ENDPOINT"));
    DB_PASSWORD = str(dotenv.get_key(dotenv_path="./.env", key_to_get="DB_PASSWORD"));
    # Use the PostgresSQL Database name from pgadmin4, not the DB identifier on Amazon RDS.
    DATABASE_NAME = str("seng550_a2_dbi");
    
    pg_conn = psql.connect(
        host=DB_ENDPOINT,
        user="postgres",
        password=DB_PASSWORD,
        database=DATABASE_NAME
    );

    # SQL Connection to database failed
    if(pg_conn.closed != 0):
        print("Connection to database failed, something went wrong...\n");
        pg_conn.rollback();
        pg_conn.close();
        return;
    # SQL Connection to database succeeded
    else:
        print(f"Connection to {DATABASE_NAME} was successful!\n");

    pg_cursor = pg_conn.cursor();

    # All the relevant data was already sorted out from the temp table.
    # NOTE: Python connection is another session, so it will NOT see the temp table.
    sql = """
        DROP TABLE IF EXISTS customer_orders CASCADE;
        CREATE TEMPORARY TABLE IF NOT EXISTS customer_orders AS
        SELECT dc.customer_id, dc.name, dc.city, fo.order_id, fo.order_date, fo.amount
        FROM dim_customers AS dc
        INNER JOIN fact_orders AS fo
            ON fo.customer_id = dc.customer_id
            AND fo.order_date BETWEEN dc.valid_start_date
            AND COALESCE(dc.valid_end_date, CURRENT_TIMESTAMP);
        -- Rename the column, because otherwise it will get deleted in the natural join
        -- and it helps to differentiate it from "name" in dim_products.
        ALTER TABLE customer_orders
        RENAME COLUMN name TO customer_name;
        SELECT *
        FROM customer_orders;

        -- Join and return the relationship attributes requested from products and orders
        DROP TABLE IF EXISTS product_orders CASCADE;
        CREATE TEMPORARY TABLE IF NOT EXISTS product_orders AS
        SELECT dp.product_id, dp.name, dp.price, fo.order_id, fo.order_date, fo.amount
        FROM dim_products AS dp
        INNER JOIN fact_orders AS fo
            ON dp.product_id = fo.product_id
            AND fo.order_date BETWEEN dp.valid_start_date
            AND COALESCE(dp.valid_end_date, CURRENT_TIMESTAMP);

        ALTER TABLE product_orders
        RENAME COLUMN name TO product_name;
        SELECT *
        FROM product_orders;

        -- Join customer_orders and product_orders ON the order columns, since they are the same
        -- in both temp tables.
        SELECT order_id, order_date, customer_id, customer_name, city AS customer_city, 
        product_id, product_name, price AS product_price, amount
        FROM customer_orders AS co
        NATURAL JOIN product_orders
    """;
    engine = create_engine(f"""postgresql+psycopg2://postgres:{DB_PASSWORD}@{DB_ENDPOINT}:5432/{DATABASE_NAME}""");

    # Use pandas to read the database query, and store them into a dataframe.
    df = pd.read_sql_query(sql, engine);

    # Transform the NaT (Not a Time, which can be NULL values), to None Type to insert into the collection.
    df = df.replace({pd.NaT: None});

    # Convert the dataframe rows to a list of dictionaries so MongoDB can read them.
    mongo_documents = df.to_dict("records");
    
    # Insert the documents into mongodb.
    orders_summary.insert_many(mongo_documents);
    print(f"Inserted {len(mongo_documents)} documents into MongoDB!");

if __name__ == "__main__":
    main();