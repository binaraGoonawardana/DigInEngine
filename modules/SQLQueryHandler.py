import os, sys
import json
from datetime import datetime, date
from time import mktime
import decimal, simplejson
import sqlalchemy as sql
from sqlalchemy import text
sys.path.append("...")
import configs.ConfigHandler as conf
from pandas import DataFrame
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
from bigquery import get_client

datasource_settings = conf.get_conf('DatasourceConfig.ini','MS-SQL')
connection_string = "mssql+pyodbc://{0}:{1}@{2}:1433/{3}?driver=SQL+Server+Native+Client+11.0"\
                    .format(datasource_settings['UID'],datasource_settings['PWD'],datasource_settings['SERVER'],
                            datasource_settings['DATABASE'],datasource_settings['DRIVER'])

engine = sql.create_engine(connection_string)
metadata = sql.MetaData()
connection = engine.connect()
class DecimalJSONEncoder(simplejson.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalJSONEncoder, self).default(o)

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))
        return json.JSONEncoder.default(self, obj)

class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        if isinstance(obj, datetime) or isinstance(obj, date):
            return obj.isoformat()
        return super(ExtendedJSONEncoder, self).default(obj)

def execute_query(query):
          data = []
          sql = text(query)
          connection = engine.connect()
          result = connection.execute(sql)
          columns = result.keys()
          print columns
          results = []
          for row in result:
               results.append(dict(zip(columns, row)))
          return    json.dumps(results, cls=ExtendedJSONEncoder)


def get_fields(datasetname, tablename):
           fields = []
           query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
           sql = text(query)
           result = connection.execute(sql)
           for row in result:
              fields.append(row[3])
           return json.dumps(fields)

def get_tables(datasetID):
          tables = []

          cursor = connection.cursor()
          query = "SELECT * FROM information_schema.tables"
          cursor.execute(query)
          rows = cursor.fetchall()
          for row in rows:
              tables.append(row[2])
          return json.dumps(tables)