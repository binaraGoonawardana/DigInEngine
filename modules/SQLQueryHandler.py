import os, sys
import json
import sqlalchemy as sql
from sqlalchemy import text
sys.path.append("...")
import configs.ConfigHandler as conf
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)

datasource_settings = conf.get_conf('DatasourceConfig.ini','MS-SQL')
connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver=SQL+Server+Native+Client+11.0"\
                    .format(datasource_settings['UID'],datasource_settings['PWD'],datasource_settings['SERVER'],
                            datasource_settings['DATABASE'],datasource_settings['DRIVER'],datasource_settings['PORT'])
try:
    engine = sql.create_engine(connection_string)
    metadata = sql.MetaData()
    connection = engine.connect()
except:
    pass

def execute_query(query):
          data = []
          sql = text(query)
          connection = engine.connect()
          result = connection.execute(sql)
          columns = result.keys()
          results = []
          for row in result:
               results.append(dict(zip(columns, row)))
          return results


def get_fields(datasetname, tablename):
           fields = []
           query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
           sql = text(query)
           result = connection.execute(sql)
           for row in result:
              fields.append(row[3])
           return fields

def get_tables(datasetID):
          tables = []
          cursor = connection.cursor()
          query = "SELECT * FROM information_schema.tables"
          cursor.execute(query)
          rows = cursor.fetchall()
          for row in rows:
              tables.append(row[2])
          return tables