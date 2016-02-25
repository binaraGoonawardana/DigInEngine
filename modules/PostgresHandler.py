__author__ = 'Sajeetharan'
import psycopg2
import datetime as dt
import logging
import web
import datetime
import json
import sys
from multiprocessing import Process, Event
import sys
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

datasource_settings = conf.get_conf('DatasourceConfig.ini','PostgreSQL')
query = ""
database = datasource_settings['DATABASE']
user = datasource_settings['USER']
password = datasource_settings['PASSWORD']
host = datasource_settings['HOST']
port = datasource_settings['PORT']

conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
logger.info('Connection made to the Digin Store Successfully')

def execute_query(querystate):
          records = []
          query = querystate
          curPG = conn.cursor('testCursor')
          curPG.itersize = 100000 # Rows fetched at one time from the server
          print("started to read data")
          print(datetime.datetime.now().time())
          cptLigne = 0
          try:
             curPG.execute(query)
             records = curPG.fetchall()
             curPG.close()
             conn.commit()
          except:
             conn.rollback()
          print("fetched data" )
          print(datetime.datetime.now().time())
          return records


def get_Fields(table_name):
          fields = []
          cursor = conn.cursor()
          query = "Select * FROM " +table_name+"  LIMIT 0"
          cursor.execute(query)
          colnames = [desc[0] for desc in cursor.description]
          return colnames


def get_Tables():
          tables = []
          cursor = conn.cursor()
          cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
          tablesWithDetails =    cursor.fetchall()
          tables =[t[0] for t in tablesWithDetails]
          return tables


