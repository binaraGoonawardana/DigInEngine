__author__ = 'Marlon Abeykoon'
__version__ = '2.0.0.2'

from memsql.common import database
import ast
#import threading
from memsql.common.query_builder import multi_insert
from memsql.common.query_builder import update
import sys
sys.path.append("...")
import configs.ConfigHandler as conf
import modules.CommonMessageGenerator as cmg
import json
import scripts.utils.DiginIDGenerator as idgen
import datetime

datasource_settings = conf.get_conf('DatasourceConfig.ini','MemSQL')
query = ""
DATABASE = datasource_settings['DATABASE']
USER = datasource_settings['USER']
PASSWORD = datasource_settings['PASSWORD']
HOST = datasource_settings['HOST']
PORT = datasource_settings['PORT']

caching_tables = conf.get_conf('CacheConfig.ini', 'Caching Tables')
tables = caching_tables['table_names']

cache_state_conf = conf.get_conf('CacheConfig.ini', 'Cache Expiration')
cache_state = int(cache_state_conf['default_timeout_interval'])

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
    if cache_state == 0: return True
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=db)



def insert_data(data,indexname,db=DATABASE):
    """
    :param data: Accepts list of dicts
    :param indexname: tablename in MEMSql
    :return:
    """
    if cache_state == 0: return True
    print 'Inserting data to cache...'
    tablename = indexname
    #TODO Check if data is null (skip take is exceeded)
    for item in data:
        try:
            item.update((k, str(v)) for k, v in item.iteritems() if k == "__osHeaders")
        except:
            print "cant update"
    sql, params = multi_insert(tablename,*data)
    with get_connection(db) as conn:
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
    if cache_state == 0: return True
    tablename = table_name
    #TODO Check if data is null (skip take is exceeded)
    sql, params = update(tablename,**data)
    sql_full = '{0} {1}'.format(sql, conditions)
    with get_connection() as conn:
             c = conn.execute(sql_full,**params)
             return c

# Method by Thivatharan for CSV table creation
def create_table(dict_fields_types,tablename,db=DATABASE,user_id=None,tenant = None):

    if cache_state == 0: return True
    print dict_fields_types
    print len(dict_fields_types)
    sql = 'CREATE TABLE %s\n(' % (tablename)
    for i in dict_fields_types:

        t = i['type']
        if t.lower() == 'string':
            # field_types[k] = 'character varying'
            sql = sql + '{0} VARCHAR(150),'.format(i['name'])
        elif t.lower() == 'integer':
            # field_types[k] = 'integer'
            sql = sql + '{0} INT,'.format(i['name'])
        elif t.lower() == 'float':
            # field_types[k] = 'NUMERIC'
            sql = sql + '{0} FLOAT,'.format(i['name'])

        elif t.lower() == 'timestamp':
            sql = sql + '{0} TIMESTAMP,'.format(i['name'])

        elif t.lower() == 'datetime':
            sql = sql + '{0} DATETIME,'.format(i['name'])

        elif t.lower() == 'date':
            sql = sql + '{0} DATE,'.format(i['name'])

        elif t.lower() == 'time':
            sql = sql + '{0} TIME,'.format(i['name'])

    QUERY_TEXT = sql[:len(sql) - 1] + '\n)'

    with get_connection(db) as conn:
        try:
             a = conn.execute(QUERY_TEXT)
             print str(a)
        except Exception, err:
             print err
             raise


    table_id = idgen.unix_time_millis_id(datetime.datetime.now())
    security_level = 'write'
    table_data = {'id': table_id,
                  'project_id': 'memsql',
                  'dataset_id': db,
                  'datasource_id': tablename,
                  'schema': json.dumps(dict_fields_types),
                  'datasource_type': 'table',
                  'created_user': user_id,
                  'created_tenant': tenant,
                  'is_active': True}

    table_access_data = {'component_id': table_id,
                         'user_id': user_id,
                         'type': 'datasource',
                         'domain': tenant,
                         'security_level': security_level
                         }
    try:
        insert_data([table_data], 'digin_datasource_details')
        insert_data([table_access_data], 'digin_component_access_details')
        print 'Table mapped to the user'
    except Exception, err:
        print err
        return False
    return table_id

def get_data(query,db=DATABASE):
    with get_connection(db) as conn:
        try:
            result_set = conn.query(query).__dict__
            return result_set
        except Exception, err:
            print err
            raise

def get_cached_data(query):
    if cache_state == 0: return True
    with get_connection() as conn:
        result_set = conn.query(query).__dict__
        return result_set

def delete_data(query,db=DATABASE):
    if cache_state == 0: return True
    with get_connection(db) as conn:
        try:
            result_set = conn.query(query)
            return result_set
        except Exception, err:
            print err
            raise

def delete_table(table_name,db=DATABASE):
    if cache_state == 0: return True
    QUERY_TEXT = 'DROP  TABLE {0}'.format(table_name)
    with get_connection(db) as conn:
        try:
            result_set = conn.query(QUERY_TEXT)
            return result_set
        except Exception, err:
            print err
            raise


def cleanup():
    """ Cleanup the database this benchmark is using. """
    if cache_state == 0: return True
    with get_connection() as conn:
        conn.query('DROP DATABASE %s' % DATABASE)

def clear_cache():

    if cache_state == 0: return True
    with get_connection() as conn:
        for table in ast.literal_eval(tables):
            try:
                conn.query("TRUNCATE TABLE {0}".format(table))
            except Exception, err:
                print "Error clearing cache"
                print err
                return cmg.format_response(False,None,"Error Occurred while clearing cache!", exception = sys.exc_info())
        return cmg.format_response(True,None,"Cache cleared successfully!")

# if __name__ == '__main__':
#     try:
#
#         warmup()
#     except KeyboardInterrupt:
#         print("Interrupted... exiting...")
    #finally:
        # cleanup()


