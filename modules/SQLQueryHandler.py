__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.1'

import os, sys
import pyodbc
from sqlalchemy import text, create_engine
sys.path.append("...")
import configs.ConfigHandler as conf
import scripts.DigINCacheEngine.CacheController as masterdb
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)

try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','MS-SQL')
    # connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver={4}"\
    #                     .format(datasource_settings['UID'],datasource_settings['PWD'],datasource_settings['SERVER'],
    #                             datasource_settings['DAT  ABASE'],datasource_settings['DRIVER'],datasource_settings['PORT'])
except Exception, err:
    print err

# try:
#     engine = sql.create_engine(connection_string)
#     metadata = sql.MetaData()
#     connection = engine.connect()
# except Exception, err:
#     print "Error connecting to sqlserver"
#     print err

def execute_query(query, datasource_config_id):
          sql_query = text(query)
          query = "SELECT  user_name, password, host_name, database_name, port_no FROM digin_data_source_config WHERE ds_config_id = {0}".format(datasource_config_id)
          result = masterdb.get_data(query)['rows'][0]
          connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver={4}" \
              .format(result[0], result[1], result[2],
                      result[3], datasource_settings['DRIVER'], result[4])
          engine = create_engine(connection_string)
          try:
            connection = engine.connect()
          except Exception, err:
              print err
          result = connection.execute(sql_query)
          columns = result.keys()
          results = []
          for row in result:
               results.append(dict(zip(columns, row)))
          return results


def get_fields(tablename, datasource_config_id):
           fields = []
           query = "SELECT  user_name, password, host_name, database_name, port_no FROM digin_data_source_config WHERE ds_config_id = {0}".format(datasource_config_id)
           result = masterdb.get_data(query)['rows'][0]
           connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver={4}" \
               .format(result[0], result[1], result[2],
                       result[3], datasource_settings['DRIVER'], result[4])
           engine = create_engine(connection_string)
           try:
               connection = engine.connect()
           except Exception, err:
               print err
           query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
           sql = text(query)
           result = connection.execute(sql)
           for row in result:
              fieldtype = {'Fieldname': row[3],
                    'FieldType':row[7]}
              fields.append(fieldtype)
           return fields

def get_tables(datasource_config_id):
          tables = []
          query = "SELECT  user_name, password, host_name, database_name, port_no FROM digin_data_source_config WHERE ds_config_id = {0}".format(datasource_config_id)
          result = masterdb.get_data(query)['rows'][0]
          connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver={4}" \
              .format(result[0], result[1], result[2],
                      result[3], datasource_settings['DRIVER'], result[4])
          engine = create_engine(connection_string)
          try:
            connection = engine.connect()
          except Exception, err:
              print err
          query = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
          result = connection.execute(query)
          for row in result:
              tables.append(row[2])
          return tables

def get_databases(params):
        try:
            engine = create_engine('mssql+pymssql://{0}:{1}@{2}:{3}'.format(params.username,params.password,params.hostname,params.port))
            conn = engine.connect()
            rows = conn.execute("SELECT name FROM sys.databases;")
        except Exception:
            print "Error Database Connection parameters!"
            raise

        list = []
        for row in rows:
            list.append((row["name"]))
        return list

def test_database_connection(params):

    try:

        connection_string='DRIVER={5};SERVER={0};PORT={1};DATABASE={2};UID={3};PWD={4}'.format(params.hostname,int(params.port),params.databasename,params.username,params.password,'{SQL Server}')
        pyodbc.connect(connection_string)
        return True

    except Exception:
        print "Error Database Connection parameters!"
        raise



