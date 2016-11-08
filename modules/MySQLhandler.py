__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import MySQLdb
import logging
import sys
import contextlib
sys.path.append("...")
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('DiginStore.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('Starting log')

try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','MySQL')
    query = ""
    user = datasource_settings['USER']
    password = datasource_settings['PASSWORD']
    host = datasource_settings['HOST']
    port = int(datasource_settings['PORT'])
except Exception, err:
    print err
    logger.error(err)

logger.info('Connection made to the Digin Store Successful')

@contextlib.contextmanager
def _get_connection(database):
    con = MySQLdb.connect(host =host, user=user, passwd=password, db=database, port=port)
    dictCursor = con.cursor(MySQLdb.cursors.DictCursor)
    try:
        yield dictCursor
    finally:
        pass

def execute_query(query, database):
    with _get_connection(database) as dictCursor:
        dictCursor.execute(query)
        resultSet = dictCursor.fetchall()

    return list(resultSet)

def get_fields(table_name, database):
    with _get_connection(database) as dictCursor:
        dictCursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}';"
                    .format(database,table_name))
    resultSet = dictCursor.fetchall()
    return resultSet

def get_tables(database):
    with _get_connection(database) as dictCursor:
        dictCursor.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='{0}'".format(database))
    result_list = []
    for resultSet in dictCursor.fetchall():
        result_list.append(resultSet['TABLE_NAME'])
    return result_list
