import sqlite3
import psycopg
import sqlalchemy


def create_engine(connection_string):
    return sqlalchemy.create_engine(connection_string)


def create_connection(db_file):
    """ create a database connection to the SQLite database specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn


def create_postgres_connection(dbname, user, password, host="localhost", port=5432):
    """ create a database connection to a PostgreSQL database
    :param dbname: database name
    :param user: database user
    :param password: database password
    :param host: database host (default: localhost)
    :param port: database port (default: 5432)
    :return: Connection object or None
    """
    conn = None
    try:
        conn = psycopg.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except psycopg.Error as e:
        print(e)
    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(e)


def insert_data(conn, table, data):
    """ insert data into a table

    :param conn: Connection object
    :param table: table name
    :param data: a tuple containing the data to be inserted
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?,?)", data)
        conn.commit()
    except sqlite3.Error as e:
        print(e)


if __name__ == "__main__":
    # sql_in = create_connection('ingest.db')
    # sql_out = create_connection('output.db')

    sql_pq = create_postgres_connection(dbname="postgres", user="postgres", password="mysecretpassword")
    print(f'Connected to {sql_pq}')
    create_table(sql_pq,
                 create_table_sql='CREATE TABLE IF NOT EXISTS test_games (Game INTEGER PRIMARY KEY, Date TEXT, HomeTeam TEXT, AwayTeam TEXT, HomeScore INTEGER, AwayScore INTEGER, Venue TEXT, Attendance INTEGER, Duration TEXT, Notes TEXT)')
