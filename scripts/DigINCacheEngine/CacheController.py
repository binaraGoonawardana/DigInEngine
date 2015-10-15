#Test this with skip take from browser take 1000
__author__ = 'Marlon'

#http://docs.memsql.com/4.0/concepts/multi_insert_examples/

from memsql.common import database
import json
import time
import threading

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
# Pre-generate the workload query
#QUERY_TEXT = "INSERT INTO %s VALUES %s" % (TABLE, ",".join([] * BATCH_SIZE))
#QUERY_TEXT = "INSERT INTO %s VALUES (1, 'Marlon')" % (TABLE)
QUERY_TEXT = ''

def get_connection(db=DATABASE):
    """ Returns a new connection to the database. """
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=db)



def insert_data(data,tablename):
    print 'inserting data...'

    #TODO Check if data is null (skip take is exceeded)
    for item in data:
        vars_to_sql = []
        #keys_to_sql = []
        print(item)
        for key,value in item.iteritems():
             if key == '__osHeaders':
                value = str(value)
             if isinstance(value, unicode):
                vars_to_sql.append(value.encode('ascii', 'ignore'))
                #keys_to_sql.append(key.encode('ascii', 'ignore'))
             else:
                vars_to_sql.append(value)
                #keys_to_sql.append(key)

        #keys_to_sql = ', '.join(keys_to_sql)
        with get_connection() as conn:
             print 'vars_to_sql'
             print vars_to_sql
             print tuple(vars_to_sql)
             #records_list_template = ','.join(['%s'] * len(vars_to_sql))
             records_list_template = ','.join(['%s'] * 1000) #TODO Batch size needs to be taken from a config
             insert_query = 'insert into {0} values {1}'.format(tablename,records_list_template)
             print 'insert query' , insert_query
             #c = conn.execute("INSERT INTO DemoHNB_claim VALUES %r" % (tuple(vars_to_sql),))
             sql = insert_query % vars_to_sql
             c = conn.execute(sql)
             print c

    # insert_sql = "Insert Into DemoHNB_claim (%s) values (%s)" #, (json.dumps(data),)
    # cols = ', '.join(data)
    # vals = ', '.join('?'* len(data))
    # to_execute = insert_sql % (cols, vals)
    # print to_execute
    #
    # with get_connection() as conn:
    #     print data.values()
    #     c = conn.execute(to_execute, data.values())
    #     print c
    return c





def warmup():
    print('Warming up workload')
    with get_connection() as conn:
        print(QUERY_TEXT)
        conn.execute(QUERY_TEXT)


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


