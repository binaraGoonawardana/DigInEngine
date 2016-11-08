__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import os, sys
import sqlalchemy as sql
from sqlalchemy import text
sys.path.append("...")
import configs.ConfigHandler as conf
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)

try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','MS-SQL')
    connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver={4}"\
                        .format(datasource_settings['UID'],datasource_settings['PWD'],datasource_settings['SERVER'],
                                datasource_settings['DATABASE'],datasource_settings['DRIVER'],datasource_settings['PORT'])
except Exception, err:
    print err

try:
    engine = sql.create_engine(connection_string)
    metadata = sql.MetaData()
    connection = engine.connect()
except Exception, err:
    print "Error connecting to sqlserver"
    print err

def execute_query(query):
          sql = text(query)
          connection = engine.connect()
          result = connection.execute(sql)
          columns = result.keys()
          results = []
          for row in result:
               results.append(dict(zip(columns, row)))
          return results


def get_fields(tablename):
           fields = []
           query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
           sql = text(query)
           result = connection.execute(sql)
           for row in result:
              fieldtype = {'Fieldname': row[3],
                    'FieldType':row[7]}
              fields.append(fieldtype)
           return fields

def get_tables(datasetID):
          tables = []
          query = "SELECT * FROM information_schema.tables"
          result = connection.execute(query)
          for row in result:
              tables.append(row[2])
          return tables