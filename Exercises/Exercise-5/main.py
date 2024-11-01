import psycopg2
import logging


# Create and configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
stream_handler = logging.StreamHandler()

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s %(message)s')
stream_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("log.log")
logger.addHandler(file_handler)


def create_table(conn, cursor, table_name: str):
    with open(f"create_{table_name}.sql", "r") as f:
        create_query = f.read()
        try:
            cursor.execute(create_query)
        except Exception as e:
            logger.exception(e)
    conn.commit()


def insert_data(conn, cursor, table_name: str):
    with open(f"data/{table_name}.csv", "r") as f:
        data = f.read()

    data_splitter = data.split('\n')
    num_of_columns = data_splitter[0].split(',')
    num_of_columns = len(num_of_columns)
    num_of_records = len(data_splitter)
    records = []
    for i in range(1, num_of_records):
        data_splitter[i] = tuple(data_splitter[i].split(','))
        records.append(data_splitter[i])
    args = ','.join(
        cursor.mogrify(f"({','.join(['%s'] * num_of_columns)})", line).decode('utf-8') for line in records)
    cursor.execute(f"INSERT INTO {table_name} VALUES " + (args))

    conn.commit()


def select_data(conn, cursor, table_name: str):
    conn.autocommit = True
    sql_query = f"select * from {table_name};"
    cursor.execute(sql_query)
    results = cursor.fetchall()
    logger.info(results)
    conn.commit()


def delete_table(conn, cursor, table_name: str):
    conn.autocommit = True
    sql_query = f"DROP TABLE IF EXISTS {table_name};"
    cursor.execute(sql_query)
    conn.commit()



def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cursor = conn.cursor()

    delete_table(conn, cursor, "transactions")
    delete_table(conn, cursor, "products")
    delete_table(conn, cursor, "accounts")

    create_table(conn, cursor, "accounts")
    create_table(conn, cursor, "products")
    create_table(conn, cursor, "transactions")

    insert_data(conn, cursor, "accounts")
    insert_data(conn, cursor, "products")
    insert_data(conn, cursor, "transactions")

    select_data(conn, cursor, "accounts")
    select_data(conn, cursor, "products")
    select_data(conn, cursor, "transactions")

    cursor.close()


if __name__ == "__main__":
    main()
