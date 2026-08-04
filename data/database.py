import sqlite3


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
    sql_in = create_connection('ingest.db')
    sql_out = create_connection('output.db')