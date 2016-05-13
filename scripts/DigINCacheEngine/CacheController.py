#Test this with skip take from browser take 1000
__author__ = 'Marlon'

#http://docs.memsql.com/4.0/concepts/multi_insert_examples/

from memsql.common import database
import json
import time
import threading
from memsql.common.query_builder import multi_insert
from memsql.common.query_builder import update
import sys
sys.path.append("...")
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('DatasourceConfig.ini','MemSQL')
query = ""
DATABASE = datasource_settings['DATABASE']
USER = datasource_settings['USER']
PASSWORD = datasource_settings['PASSWORD']
HOST = datasource_settings['HOST']
PORT = datasource_settings['PORT']

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
    print 'Inserting data to cache...'
    tablename = indexname
    #TODO Check if data is null (skip take is exceeded)
    for item in data:
        try:
            item.update((k, str(v)) for k, v in item.iteritems() if k == "__osHeaders")
        except:
            print "cant update"
    sql, params = multi_insert(tablename,*data)
    with get_connection() as conn:
        try:
             c = conn.execute(sql,**params)
        except Exception, err:
            print err
            raise
        print 'Cache insertion successful!'
        return c

def update_data(table_name, conditions, **data):
    """
    :param data: Accepts list of dicts
    :param table_name: tablename in MEMSql
    :return:
    """
    print 'updating data...'
    tablename = table_name
    #TODO Check if data is null (skip take is exceeded)
    sql, params = update(tablename,**data)
    sql_full = '{0} {1}'.format(sql, conditions)
    print 'sql', sql_full
    with get_connection() as conn:
             c = conn.execute(sql_full,**params)
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

def get_data(query):
    with get_connection() as conn:
        result_set = conn.query(query).__dict__
        return result_set

def delete_data(query):
    with get_connection() as conn:
        result_set = conn.query(query)
        return result_set


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


