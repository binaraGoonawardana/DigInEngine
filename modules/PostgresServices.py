__author__ = 'Sajeetharan'
import psycopg2
import datetime as dt
import logging
import web
import datetime
import json
import psycopg2.extras
from multiprocessing import Process, Event
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('DiginStore.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('Starting log')
urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables'

)

app = web.application(urls, globals())
import sys
sys.path.append("...")
import configs.ConfigHandler as conf
datasource_settings = conf.get_conf('DatasourceConfig.ini','PostgreSQL')
query = ""
database = datasource_settings['DATABASE']
user = datasource_settings['USER']
password = datasource_settings['PASSWORD']
host = datasource_settings['HOST']
port = datasource_settings['PORT']

conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
logger.info('Connection made to the Digin Store Successfully')
class execute_query:
    def GET(self,r):
          records = []
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          query = web.input().query
          print("started to read data")
          print(datetime.datetime.now().time())
          cptLigne = 0
          try:
             cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
             cur.execute (query)
             ans =cur.fetchall()
             for row in ans:
                records.append(dict(row))
             conn.commit()
          except Exception, msg:
             conn.rollback()
          return records


class get_Fields:
   def GET(self,r):
          web.header('Access-Control-Allow-Origin','*')
          web.header('Access-Control-Allow-Credentials', 'true')
          fields = []
          tablename = web.input().tableName
          cursor = conn.cursor()
          query = "Select * FROM " +tablename+"  LIMIT 0"
          cursor.execute(query)
          colnames = [desc[0] for desc in cursor.description]
          return json.dumps(colnames)


class get_Tables:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          tables = []
          cursor = conn.cursor()
          cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
          tablesWithDetails =    cursor.fetchall()
          tables =[t[0] for t in tablesWithDetails]
          return json.dumps(tables)


if  __name__ == "__main__":
      app.run()