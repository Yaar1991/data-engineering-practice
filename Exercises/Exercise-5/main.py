import psycopg2

"""
Problems Statement
- There is a folder called data in this current directory, Exercises/Exercise-5. There are also 3 csv files located in that folder. 
  Open each one and examine it, the first task is to create a sql script with the DDL to hold a CREATE statement for each data file. 

- Remember to think about data types. 

- Also, this CREATE statements should include indexes for each table, as well as primary and foreign keys.

- After you have finished this sql scripts, we must connect to Postgres using the Python package called psycopg2. 
  Once connected we will run our sql scripts against the database.

- Note: The default main.py script already has the Python connection configured to connect to the Postgres instance that 
  is automatically spun up by Docker when you ran the docker-compose up run command (inside Exercises/Exercise-5 directory).

- Finally, we will use psycopg2 to insert the data in each csv file into the table you created.


Generally, your script should do the following ...

Examine each csv file in data folder. Design a CREATE statement for each file.
Ensure you have indexes, primary and forgein keys.
Use psycopg2 to connect to Postgres on localhost and the default port.
Create the tables against the database.
Ingest the csv files into the tables you created, also using psycopg2."""


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here
    with open("create_accounts.sql", "r") as f:
        create_accounts = f.read()
        try:
            conn.cursor().execute(create_accounts)
        except Exception as e:
            print(e)
    with open("create_products.sql", "r") as f:
        create_products = f.read()
        try:
            conn.cursor().execute(create_products)
        except Exception as e:
            print(e)
    with open("create_transactions.sql", "r") as f:
        create_transactions = f.read()
        try:
            conn.cursor().execute(create_transactions)
        except Exception as e:
            print(e)

    conn.commit()
    conn.cursor().close()


if __name__ == "__main__":
    main()
