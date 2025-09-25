# SENG 550 - Assignment 1

## Carlos Morera Pinilla

UCID: 30113818

## Setup Tips before running the script:

- Please create a parent directory for `A1` directory given. All paths required for some functions, and the paths to things explained in this README file are relative to this parent directory.

- Included in `A1` directory, is a child directory called `SQL-Queries` which are the SQL queries I ran in Postgres pgadmin4 for Part 4 of the assignment. Please run these queries in pgadmin4, as they were not executed in the Python script.

- The script depends on `./A1/SQL-Queries/create-tables.sql` query to be run FIRST. So please run this in your pgadmin server before running the Python script and ensure the tables are created.

- Within your parent directory, create a `.env` file with the following fields:

  - `DB_ENDPOINT`: The endpoint URL, if you set up a server for your database outside of localhost, this is not used if you specify `localhost` as your endpoint.

  - `DB_PASSWORD`: The master/root password for the database server you set up.

- The Python script to run is `load.py`, and if you use a parent directory, you can run the program with this command:

```bash
py "./A1/load.py"
```

## Running the Python script:

1. When you run the program, it will prompt you for a bulk delete. Enter `0` to say No, or enter `1` to say Yes.

2. It is HIGHLY recommended to run option `1` first, and then on the next run, run option `0`. This ensures you always run on a fresh set of tables and do not create large tables from subsequent runs with potential duplicate data.

3. Then it will prompt you for the endpoint. Copy-paste it in, or if you are using localhost, just type `localhost`. (NOTE: If you type localhost, you don't have to define the `DB_ENDPOINT` variable; it will just use localhost as a string literal). In either case, you must supply the master password for the database server you used to configure the database.

4. After typing the endpoint or localhost, the program will run and execute some sample queries from the Assignment Specifications.

5. You can see the results of the queries from this script, in pgAdmin using the `./A1/SQL-Queries/select-statements.sql` file. It should return EMPTY, when you run option 1 for bulk deletion, and have a populated table when you run option 0 AFTER running option 1.

6. **PLEASE MAKE SURE YOU RUN THE `create-tables.sql` QUERY IN PGADMIN4 PRIOR TO RUNNING THIS SCRIPT.**

## Part 4 - SQL QUERIES:

- This part of the assignment was done on pgadmin, so you can ignore the Python script from here.

- Please only run these queries AFTER the Python script finishes executing.

- If you would like to run these, run them on pgadmin Query editor.

- For Part 4, included in `./A1/SQL-Queries` directory, and numbered 1 to 8 are the corresponding SQL commands we were required to run to make queries to the database.

- The `select.statements.sql` file is just a collection of SELECT \* statements for each of the tables to see the tables after an insertion or deletion operation. These are simply for your reference.

## Part 5 - Adjusting the Schema:

- This portion of the assignment was also done in pgadmin4.

- In this section, I only created the tables as it was NOT specified in the assignment document that we had to populate the tables.

- To see what I did, run in pgadmin this SQL file in the Query editor: `./A1/SQL-Queries/A1-Part5.sql`.
