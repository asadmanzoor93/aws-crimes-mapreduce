# MySQL database connection module.
# this connects to a database and executes queries.
# this does not do any connection pooling.
import mysql.connector
import logging


def get_connection():
    """
    get MySQL connection.  auto-commit is set to False by default.  you don't need to
    start a transaction explicitly
    :param environment: the environment
    :param read_only: whether to connect to read-only db or not
    :param db: name of the database
    :return: the database connection
    """
    try:
        config = {
            'user': 'asad',
            'password': 'admin123',
            'host': 'myawsbigdatainstance.cmioopaskdwr.us-east-1.rds.amazonaws.com',
            'database': 'crimes_database'
        }

        cn = mysql.connector.connect(**config)
        return cn
    except KeyError:
        print("While connecting to a server, a key to generate the connection was not found")
    except Exception as e:
        logging.exception(e)
        print("A connection to {0} was not made", 'aws-bigdata-database.cmdcmhykbbuv.us-east-1.rds.amazonaws.com')


def execute_query_with_cnx(cnx, sql, parameters=(), fetch_one=False, close_connection=True):
    """
    execute SQL query with given connection
    :param close_connection: if True close the connection after the query is ran
    :param cnx: database connection you opened explicitly
    :param sql: SQL query to execute
    :param parameters: parameters - defaults to empty
    :param fetch_one: fetch only single record
    :return:
    """
    # return dictionary rather than tuple
    cursor = cnx.cursor(dictionary=True)
    cursor.execute(sql, parameters)
    everything = cursor.fetchone() if fetch_one else cursor.fetchall()
    cursor.close()

    if close_connection:
        cnx.close()
    return everything


def execute_update_with_cnx(cnx, sql, parameters=()):
    """
    execute SQL update with given connection.
    Wasted a lot of time in it so adding this as a reminder, please commit your changes manually.
    :param cnx: DB connection
    :param sql: SQL query to execute
    :param parameters: parameters - defaults to empty
    :return:
    """
    cursor = cnx.cursor()
    cursor.execute(sql, parameters)
    cursor.close()

