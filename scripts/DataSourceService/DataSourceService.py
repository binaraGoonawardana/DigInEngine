__author__ = 'Marlon Abeykoon'
__version__ = '1.1.0'

import json
from bigquery import get_client
import sys
import sqlalchemy as sql
from sqlalchemy import text
import logging
import datetime
import re
from multiprocessing import Process
import modules.CommonMessageGenerator as comm
sys.path.append("...")
import configs.ConfigHandler as conf
import scripts.DigINCacheEngine.CacheController as CC

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/DatasourceService.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
import psycopg2
import psycopg2.extras
logger.addHandler(handler)

logger.info('Starting log')

try:
    datasource_settings_bq = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
    query = ""
    project_id = datasource_settings_bq['PROJECT_ID']
    service_account = datasource_settings_bq['SERVICE_ACCOUNT']
    key = datasource_settings_bq['KEY']
except Exception, err:
    print err
    logger.error(err)
    pass

try:
    datasource_settings_mssql = conf.get_conf('DatasourceConfig.ini','MS-SQL')
    logger.info(datasource_settings_mssql)
    connection_string = "mssql+pyodbc://{0}:{1}@{2}:{5}/{3}?driver=SQL+Server+Native+Client+11.0"\
                        .format(datasource_settings_mssql['UID'],datasource_settings_mssql['PWD'],datasource_settings_mssql['SERVER'],
                                datasource_settings_mssql['DATABASE'],datasource_settings_mssql['DRIVER'],datasource_settings_mssql['PORT'])
    logger.info(connection_string)
except Exception, err:
    print err
    logger.error(err)
    pass

try:
    engine = sql.create_engine(connection_string)
    metadata = sql.MetaData()
    connection = engine.connect()
except Exception, err:
    logger.error(err)
    pass
try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','PostgreSQL')
    query = ""
    database = datasource_settings['DATABASE']
    user = datasource_settings['USER']
    password = datasource_settings['PASSWORD']
    host = datasource_settings['HOST']
    port = datasource_settings['PORT']
except Exception, err:
    print err
    logger.error(err)
    pass
try:
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
except psycopg2.InterfaceError as exc:
     print exc.message
     conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
     pass
except Exception, err:
     logger.error(err)
     pass
logger.info('Connection made to the Digin Store Successfully')

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']

def MEM_insert(id, data, query, cache_timeout):
        logger.info("Cache insertion started...")
        createddatetime = datetime.datetime.now()
        expirydatetime = createddatetime + datetime.timedelta(seconds=cache_timeout)

        to_cache = [{'id': str(id),
                     'data': data,
                     'query': query,
                     'expirydatetime': expirydatetime,
                     'createddatetime': createddatetime}]

        try:
            CC.insert_data(to_cache,'cache_execute_query')

        except Exception, err:
            logger.error("Error inserting to cache!")
            logger.error(err)
            pass

def execute_query(params, cache_key):

          try:
            limit_ = int(params.limit)
          except:
            limit_ = int(1000)
            pass
          try:
            offset_ = params.offset
          except:
            offset_ = None
            pass
          query = params.query
          db = params.db
          columns = []
          try:
            cache_timeout = int(params.t)
          except AttributeError, err:
            logger.info("No cache timeout mentioned.")
            cache_timeout = int(default_cache_timeout)

          time = datetime.datetime.now()
          try:
                cache_existance = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_execute_query WHERE id = '{1}'".format(time, cache_key))['rows']
          except Exception, err:
                logger.error("Error connecting to cache..")
                logger.error(err)
                cache_existance = ()
                pass
          if len(cache_existance) != 0:
                try:
                    data = CC.get_cached_data("SELECT data, query FROM cache_execute_query WHERE id = '{0}'".format(cache_key))['rows']
                except Exception,err:
                    return  comm.format_response(False,None,"Error occurred while retrieving data from cache!",exception=sys.exc_info())
                return  comm.format_response(True,json.loads(data[0][0]),data[0][1],exception=None)


          if db.lower() == 'bigquery':
               #limited_query = query + ' ' + 'limit ' + str(limit_)
               client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
               #job_id, _results = client.query(limited_query)
               job_id, _results = client.query(query)
               #complete, row_count = client.check_job(job_id)
               results = client.get_query_rows(job_id,offset=offset_,limit =limit_)
               try:
                    logger.info('Inserting to cache..')
                    p = Process(target=MEM_insert,args=(cache_key,json.dumps(results),query,cache_timeout))
                    p.start()
               except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
               return  comm.format_response(True,results,query,exception=None)

          elif db.lower() == 'mssql':
               sql = text(query)
               sql = re.sub(r'(SELECT)', r'\1 TOP {0} '.format(limit_), '{0}'.format(sql), count=1, flags=re.IGNORECASE)
               result = connection.execute(sql)
               columns = result.keys()
               results = []
               for row in result:
                  results.append(dict(zip(columns, row)))
               try:
                    logger.info('Inserting to cache..')
                    p = Process(target=MEM_insert,args=(cache_key,json.dumps(results),query,cache_timeout))
                    p.start()
               except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
               return  comm.format_response(True,results,query,exception=None)

          elif db.lower() == 'postgresql':
              data = []
              if offset_ is not None:
                  query +=  ' OFFSET ' + str(offset_)
              query +=  ' LIMIT ' + str(limit_)

              try:
                 cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                 cur.execute(query)
                 conn.commit()
                 ans =cur.fetchall()
                 for row in ans:
                    data.append(dict(row))
              except Exception, msg:
                 conn.rollback()
              try:
                    logger.info('Inserting to cache..')
                    p = Process(target=MEM_insert,args=(cache_key,json.dumps(data),query,cache_timeout))
                    p.start()
              except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
              return  comm.format_response(True,data,query,exception=None)

          else:
               return "db not implemented"

def get_fields(params):

          fields = []
          tablename = params.tableName
          db = params.db

          if db.lower() == 'bigquery':
                datasetname = params.dataSetName
                client = get_client(project_id, service_account=service_account,private_key_file=key, readonly=True)
                results = client.get_table_schema(datasetname,tablename)
                for x in results:
                  fieldtype = {'Fieldname': x['name'],
                        'FieldType':x['type']}
                  fields.append(fieldtype)
                return  comm.format_response(True,fields,"",exception=None)
          elif db.lower() == 'mssql':
                fields = []
                tablename = params.tableName
                query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
                sql = text(query)
                result = connection.execute(sql)
                for row in result:
                  fieldtype = {'Fieldname': row[3],
                        'FieldType':row[7]}
                  fields.append(fieldtype)
                return comm.format_response(True,fields,"",exception=None)
          elif db.lower() == 'postgresql':
                schema_name = params.schema
                cursor = conn.cursor()
                query = "SELECT column_name, data_type FROM information_schema.columns " \
                        "WHERE table_schema = '{0}' " \
                        "AND table_name = '{1}'".format(schema_name, tablename)
                cursor.execute(query)
                colnames =[]
                for desc in cursor:
                   field ={'Fieldname': desc[0],
                            'FieldType': desc[1]}
                   colnames.append(field)
                return  comm.format_response(True,colnames,"",exception=None)
          else:
                return "db not implemented"


def get_tables(params):

          tables = []
          datasetID = params.dataSetName
          db = params.db
          if db.lower() == 'bigquery':
              try:
                  client = get_client(project_id, service_account=service_account,
                                private_key_file=key, readonly=True)
                  result  = client.get_all_tables(datasetID)
              except Exception, err:
                  return  comm.format_response(False,err,"Error Occurred when retrieving tables!",exception=sys.exc_info())
              return  comm.format_response(True,result,"Tables retrieved!",exception=None)
          elif db.lower() == 'mssql':
              tables = []
              datasetID = params.dataSetName
              #connection_string = 'DRIVER={SQL Server};SERVER=192.168.1.83;DATABASE='+ datasetID +';UID=smsuser;PWD=sms';
              query = "SELECT * FROM information_schema.tables"
              result = connection.execute(query)
              for row in result:
                tables.append(row[2])
              return  comm.format_response(True,tables,"",exception=None)
          elif db.lower() == 'postgresql':
              tables = []
              cursor = conn.cursor()
              cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
              tablesWithDetails = cursor.fetchall()
              tables =[t[0] for t in tablesWithDetails]
              return comm.format_response(True,tables,"",exception=None)
          else:
              return "db not implemented"


def create_Dataset(params):
          datasetID = params.dataSetName
          db = params.db
          if db == 'bigquery':
              client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
              datasetname = datasetID
              try:
               result  = client.create_dataset(datasetID,None,None,None)
               return  comm.format_response(True,True,"",exception=None)
              except Exception, err:
               return False
          elif db == 'MSSQL':
              tables = []
          else:
              return "db not implemented"