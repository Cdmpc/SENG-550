# SENG 550 - Assignment 2

## Carlos Morera Pinilla

UCID: 30113818

## Setup Tips BEFORE running part2.py script:

- Please create a parent directory for `A2` directory given. All paths required for some functions, and the paths to things explained in this README file are relative to this parent directory.

- Included in `A2` directory, is a child directory called `SQL` which are the SQL queries for Part3 of this assignment.

- The script depends on `./A2/SQL/create-2d-tables.sql` query to be run FIRST. So please run this in your pgadmin server before running the Python script and ensure the tables are created.

- This database is hosted on Amazon RDS, you can create your own RDS instance and use it's endpoint and PostgreSQL port of 5432.

- The Python script to run is `part2.py`, and if you use a parent directory, you can run the program with this command:

```bash
py "./A2/part2.py"
```

## Running the Python script:

1. When you run the program, it will prompt you for a bulk delete. Enter `2` to say No, or enter `1` to say Yes.

2. It is HIGHLY recommended to run option `1` first, and then on the next run, run option `2`. This ensures you always run on a fresh set of tables and do not create large tables from subsequent runs with potential duplicate data.

3. Then it will prompt you for the endpoint. Copy-paste in the database endpoint.

4. After typing the endpoint the program will run and execute some of the queries with the basic data, including all the update and addition
   queries to the Type 2 SCD database.

5. You can see the results of the queries from this script, in pgAdmin using the `./A2/SQL/selects.sql` file. It should return EMPTY, when you run option 1 for bulk deletion, and have a populated table when you run option 2 AFTER running option 1.

6. **PLEASE MAKE SURE YOU RUN THE `create-2d-tables.sql` QUERY IN PGADMIN4 PRIOR TO RUNNING THIS SCRIPT.**

## Part 3 - Analysis Queries:

- This part of the assignment was done on pgadmin, so you can ignore the Python script from here.

- Please only run these queries AFTER the Python script finishes executing.

- If you would like to run these, run them on pgadmin Query editor or an equivalent envrionment that supports running these PostgreSQL queries.

- For Part 3, included in `./A2/SQL` directory, is the file `Part3.sql`, which contain the 4 queries needed for this assignment. They will be numbered
  1.) to 4.) So please read the comments carefully and only run these queries one at a time.

- The `selects.sql` file is just a collection of SELECT \* statements to see the state of the tables when you run the python script. These may be useful for reference.

## Part 4 & 5 - MongoDB Setup and MongoDB Queries:

- The MongoDB Collection is hosted on an Atlas Cluster and accessed using Compass.

- Included in this path: `A2\scripts\etl.py`, is a Python Script that connects to both the Atlas Server and the PostgreSQL server and populates or bulk deletes the MongoDB Collection `orders_summary` in `sales_db`. The Part3 SQL Queries MUST be run first in order for this script to work so please run those in pgadmin4.
