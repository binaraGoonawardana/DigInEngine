import os, sys
import json
import web
import pyodbc
from pandas import DataFrame
#code added by sajee on 12/27/2015
sys.path.append("...")
import configs.ConfigHandler as conf
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
from bigquery import get_client

urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables'
)
datasource_settings = conf.get_conf('DatasourceConfig.ini','MS-SQL')
connection_string = "DRIVER={{{0}}};SERVER={1};DATABASE={2};UID={3};PWD={4}"\
                    .format(datasource_settings['DRIVER'],datasource_settings['SERVER'],datasource_settings['DATABASE'],
                            datasource_settings['UID'],datasource_settings['PWD'])
app = web.application(urls, globals())


class execute_query:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          cnxn = pyodbc.connect(connection_string)
          data = []
          cursor = cnxn.cursor()
          query = web.input().query
          cursor.execute(query)
          rows = cursor.fetchall()
          columns = [column[0] for column in cursor.description]
          print columns
          results = []
          for row in rows:
              results.append(dict(zip(columns, row)))
          return json.dumps(results)



class get_Fields:
   def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          fields = []
          datasetname = web.input().datasetName
          tablename = web.input().tableName
          cnxn = pyodbc.connect(connection_string)
          cursor = cnxn.cursor()
          column_data = cursor.columns(table=tablename, catalog=datasetname, schema='dbo').fetchall()
          for row in column_data:
              fields.append(row[3])
          return json.dumps(fields)

class get_Tables:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          tables = []
          datasetID = web.input().dataSetID
          cnxn = pyodbc.connect(connection_string)
          cursor = cnxn.cursor()
          query = "SELECT * FROM information_schema.tables"
          cursor.execute(query)
          rows = cursor.fetchall()
          for row in rows:
              tables.append(row[2])
          return json.dumps(tables)

if  __name__ == "__main__":
        app.run()