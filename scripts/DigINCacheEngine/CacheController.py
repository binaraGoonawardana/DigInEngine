#Test this with skip take from browser take 1000
__author__ = 'Marlon'

#http://docs.memsql.com/4.0/concepts/multi_insert_examples/

from memsql.common import database
import json
import time
import threading
from memsql.common.query_builder import multi_insert

HOST = "104.236.192.147" #TODO Take from config
PORT = 3306
USER = "root"
PASSWORD = ""

DATABASE = "DiginCacheDB"
TABLE = "datastore"


# The number of workers to run
NUM_WORKERS = 20

# Run the workload for this many seconds
WORKLOAD_TIME = 10

# Batch size to use
BATCH_SIZE = 5000
#VALUES = "1"

QUERY_TEXT = ''

def get_connection(db=DATABASE):
    """ Returns a new connection to the database. """
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=db)



def insert_data(data,indexname):
    """
    :param data: Accepts list of dicts
    :param indexname: tablename in MEMSql
    :return:
    """
    print 'inserting data...'
    tablename = indexname
    #TODO Check if data is null (skip take is exceeded)
    for item in data:
        try:
            item.update((k, str(v)) for k, v in item.iteritems() if k == "__osHeaders")
        except:
            print "cant update"
    sql, params = multi_insert(tablename,*data)
    print 'sql', sql
    with get_connection() as conn:
             c = conn.execute(sql,**params)
             print c
             return c

def create_table(dict_fields_types,tablename):
    print dict_fields_types
    print len(dict_fields_types)
    records_list_template = ','.join(['(%s)'] * len(dict_fields_types))
    QUERY_TEXT = "CREATE TABLE IF NOT EXISTS %s ( " % (tablename)
    QUERY_TEXT1 = QUERY_TEXT+ '{0} );'.format(records_list_template)
    print QUERY_TEXT1
    with get_connection() as conn:
        c = conn.execute(QUERY_TEXT)
        return c

def get_data(tablename,fieldnames,conditions):
    if conditions is None or conditions == '':
        conditions = '1=1'
    if fieldnames is None or fieldnames == '':
        fieldnames = '*'
    with get_connection() as conn:
        result = conn.get('SELECT {0} FROM {1} WHERE {2} ;'.format(fieldnames,tablename,conditions))
        return result


def cleanup():
    """ Cleanup the database this benchmark is using. """

    with get_connection() as conn:
        conn.query('DROP DATABASE %s' % DATABASE)

# if __name__ == '__main__':
#     try:
#
#         warmup()
#     except KeyboardInterrupt:
#         print("Interrupted... exiting...")
    #finally:
        # cleanup()


