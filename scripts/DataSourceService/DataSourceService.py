__author__ = 'Marlon Abeykoon'
__version__ = '1.1.1'

import json
import sys
from sqlalchemy import text
import logging
import datetime
import threading
import re
# from multiprocessing import Process
sys.path.append("...")
import modules.CommonMessageGenerator as comm
import modules.MySQLhandler as mysqlhandler
import modules.SQLQueryHandler as mssqlhandler
import modules.PostgresHandler as pgsqlhandler
import modules.BigQueryHandler as bqhandler
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
logger.addHandler(handler)

logger.info('Starting log')

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
            print err
            logger.error("Error inserting to cache!")
            logger.error(err)

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
               results = bqhandler.execute_query(query, offset=offset_, limit=limit_)
               try:
                    logger.info('Inserting to cache..')
                    # p = Process(target=MEM_insert,args=(cache_key,json.dumps(results),query,cache_timeout))
                    # p.start()
                    t = threading.Thread(target=MEM_insert, args=(cache_key,json.dumps(results),query,cache_timeout))
                    t.start()
               except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
               return  comm.format_response(True,results,query,exception=None)

          elif db.lower() == 'mssql':
               sql = text(query)
               sql = re.sub(r'(SELECT)', r'\1 TOP {0} '.format(limit_), '{0}'.format(sql), count=1, flags=re.IGNORECASE)
               result = mssqlhandler.execute_query(sql)
               try:
                    logger.info('Inserting to cache..')
                    # p = Process(target=MEM_insert,args=(cache_key,json.dumps(result),query,cache_timeout))
                    # p.start()
                    t = threading.Thread(target=MEM_insert, args=(cache_key,json.dumps(result),query,cache_timeout))
                    t.start()
               except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
               return  comm.format_response(True,result,query,exception=None)

          elif db.lower() == 'postgresql':
              if offset_ is not None:
                  query +=  ' OFFSET ' + str(offset_)
              query +=  ' LIMIT ' + str(limit_)
              data = pgsqlhandler.execute_query(query)
              try:
                    logger.info('Inserting to cache..')
                    # p = Process(target=MEM_insert,args=(cache_key,json.dumps(data),query,cache_timeout))
                    # p.start()
                    t = threading.Thread(target=MEM_insert, args=(cache_key,json.dumps(data),query,cache_timeout))
                    t.start()
              except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
              return  comm.format_response(True,data,query,exception=None)

          elif db.lower() == 'mysql':
                try:
                    resultSet = mysqlhandler.execute_query(query,params.db_name)
                except Exception, err:
                    print err
                    raise
                try:
                    logger.info('Inserting to cache..')
                    # p = Process(target=MEM_insert,args=(cache_key,json.dumps(resultSet),query,cache_timeout))
                    # p.start()
                    t = threading.Thread(target=MEM_insert, args=(cache_key,json.dumps(resultSet),query,cache_timeout))
                    t.start()
                except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
                return comm.format_response(True,resultSet,query,exception=None)
          else:
               return "db not implemented"

def get_fields(params):

          tablename = params.tableName
          db = params.db

          if db.lower() == 'bigquery':
                datasetname = params.dataSetName
                fields = bqhandler.get_fields(datasetname, tablename)
                return  comm.format_response(True,fields,"",exception=None)
          elif db.lower() == 'mssql':
                fields = mssqlhandler.get_fields(tablename)
                return comm.format_response(True,fields,"",exception=None)
          elif db.lower() == 'postgresql':
                schema_name = params.schema
                colnames = pgsqlhandler.get_fields(tablename,schema_name)
                return comm.format_response(True,colnames,"",exception=None)
          elif db.lower() == 'mysql':
                colnames = mysqlhandler.get_fields(params.tableName)
                return comm.format_response(True,colnames,"",exception=None)
          else:
                return comm.format_response(False,db,"DB not implemented!",exception=None)


def get_tables(params):

          datasetID = params.dataSetName
          db = params.db
          if db.lower() == 'bigquery':
              try:
                  result = bqhandler.get_table(datasetID)
              except Exception, err:
                  return  comm.format_response(False,err,"Error Occurred when retrieving tables!",exception=sys.exc_info())
              return  comm.format_response(True,result,"Tables retrieved!",exception=None)
          elif db.lower() == 'mssql':
              datasetID = params.dataSetName
              tables = mssqlhandler.get_tables(datasetID)
              return  comm.format_response(True,tables,"",exception=None)
          elif db.lower() == 'postgresql':
              tables = pgsqlhandler.get_Tables()
              return comm.format_response(True,tables,"",exception=None)
          elif db.lower() == 'mysql':
              tables = mysqlhandler.get_tables(datasetID)
              return comm.format_response(True,tables,"",exception=None)
          else:
              return "db not implemented"


def create_Dataset(params):
          datasetID = params.dataSetName
          db = params.db
          if db.lower() == 'bigquery':
              try:
                   result = bqhandler.create_dataset(datasetID)
                   return  comm.format_response(True,result,"",exception=None)
              except Exception, err:
                   print err
                   return False
          else:
              return "db not implemented"