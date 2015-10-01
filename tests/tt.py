__author__ = 'Marlon'

from memsql.common import database
import time
import threading

HOST = "104.236.192.147"
PORT = 3306
USER = "root"
PASSWORD = ""

DATABASE = "DiginCacheDB"
TABLE = "datastoreLoad"


QUERY_TEXT = "INSERT INTO %s (value) VALUES ('Marlon')" %  (TABLE)


def get_connection(db=DATABASE):
    """ Returns a new connection to the database. """
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=db)


def setup_test_db():
    """ Create a database and table for this benchmark to use. """

    with get_connection(db="information_schema") as conn:
        print('Creating database %s' % DATABASE)
        conn.query('CREATE DATABASE IF NOT EXISTS %s' % DATABASE)
        conn.query('USE %s' % DATABASE)

        print('Creating table %s' % TABLE)
        conn.query('CREATE TABLE IF NOT EXISTS %s (ppkey INT (8) not null AUTO_INCREMENT, value varchar (2000), PRIMARY KEY(ppkey) )' % (TABLE) )


def insert():
    print('inserting')
    with get_connection() as conn:
        print(QUERY_TEXT)
        conn.execute(QUERY_TEXT)

if __name__ == '__main__':
    try:
        setup_test_db()
        insert()
    except KeyboardInterrupt:
         print "error"
