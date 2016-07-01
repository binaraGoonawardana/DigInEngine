__author__ = 'Manura Omal Bhagya'

import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as postgres
import modules.CommonMessageGenerator as cmg
import pandas as pd
import scripts.DigINCacheEngine.CacheController as CC
import configs.ConfigHandler as conf
import datetime
import logging
import json
from multiprocessing import Process
import modules.FuzzyCAlgo as fc
import modules.kmeans_algo as ka

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/DiginAlgo.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('Starting log')

def ret_data(dbtype, rec_data):

    df = pd.DataFrame()
    for i in range(0, len(rec_data)):
        tables = rec_data[i].keys()
        fields = rec_data[i].values()
        fields = fields[0]

        fields_str = ', '.join(fields)
        tables_str = ', '.join(tables)

        if dbtype.lower() == 'mssql':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
                result = mssql.execute_query(query)

            except Exception, err:
                logger.error(err)
                result = cmg.format_response(False, None, 'Error occurred while getting data from MSSQL!', sys.exc_info())
                return result

        elif dbtype.lower() == 'bigquery':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str, tables_str)
                result = BQ.execute_query(query)

            except Exception, err:
                logger.error(err)
                result = cmg.format_response(False, None, 'Error occurred while getting data from BigQuery Handler!',
                                             sys.exc_info())
                return result

        elif dbtype.lower() == 'pgsql':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str, tables_str)
                result = postgres.execute_query(query)

            except Exception, err:
                logger.error(err)
                result = cmg.format_response(False, None, 'Error occurred while getting data from Postgres Handler!',
                                             sys.exc_info())
                return result

        if df.empty:
            df = pd.DataFrame(result)
        else:
            df1 = pd.DataFrame(result)
            df = pd.concat([df, df1], axis=1)

    return df


def cache_data(output, id, cache_timeout, name_algo):

    logger.info("Cache insertion started...")
    createddatetime = datetime.datetime.now()
    expirydatetime = createddatetime + datetime.timedelta(seconds=cache_timeout)

    to_cache = [{'id': id, 'name_algo': name_algo, 'data': json.dumps(output), 'expirydatetime': expirydatetime,
                 'createdatetime': createddatetime}]

    try:
        p = Process(target=CC.insert_data,args=(to_cache, 'cache_algorithms'))
        logger.info('cache insertion is progressing')
        p.start()
    except Exception, err:
        logger.error("Error inserting to cache!")
        logger.error(err)
        pass

def ret_kmeans(dbtype, rec_data, id, cache_timeout):

    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_algorithms "
                                      "WHERE id = '{1}' and name_algo='kmeans'".format(time, id))['rows']

    except Exception, err:
        logger.error("Error connecting to cache..")
        cache_existance = ()
        pass

    if len(cache_existance) == 0 or cache_existance[0][0] == 0:
        df = ret_data(dbtype, rec_data)

        try:
            output = ka.kmeans_algo(df)
            cache_data(output, id, cache_timeout, name_algo='kmeans')
            result = cmg.format_response(True,output,'Kmeans processed successfully!')

        except Exception, err:
            logger.error(err)
            result = cmg.format_response(False,None,'Kmeans Failed!', sys.exc_info())

        finally:
            return result

    else:
        logger.info("Getting Kmeans data from Cache..")
        result = ''
        try:
            data = json.loads(CC.get_cached_data("SELECT data FROM cache_algorithms "
                                          "WHERE id = '{0}' and name_algo='kmeans'".format(id))['rows'][0][0])
            result = cmg.format_response(True,data,'Data successfully processed!')
            logger.info("Data received from cache")
        except:
            logger.error("Error occurred while fetching data from Cache")
            result = cmg.format_response(False,None,'Error occurred while getting data from cache!',sys.exc_info())
            raise
        finally:
            return result

def ret_fuzzyC(dbtype, rec_data, id, cache_timeout):
    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_data("SELECT expirydatetime >= '{0}' FROM cache_algorithms "
                                      "WHERE id = '{1}' and name_algo='fuzzyC'".format(time, id))['rows']

    except Exception, err:
        logger.error("Error connecting to cache..")
        cache_existance = ()
        pass

    if len(cache_existance) == 0 or cache_existance[0][0] == 0:
        df = ret_data(dbtype, rec_data)
        print 'dbtype', dbtype
        print 'rec_data', rec_data
        #print'df=',df
        try:
            output =fc.FuzzyC_algo(df)
            #output=ac.AgglomerativeClustering(df)
            cache_data(output, id, cache_timeout, name_algo='fuzzyC')
            result = cmg.format_response(True,output,'fuzzyC processed successfully!')

        except Exception, err:
            logger.error(err)
            result = cmg.format_response(False,None,'fuzzyC Failed!', sys.exc_info())

        finally:
            return result

    else:
        logger.info("Getting fuzzyC data from Cache..")
        result = ''
        try:
            data = json.loads(CC.get_data("SELECT data FROM cache_algorithms "
                                          "WHERE id = '{0}' and name_algo='fuzzyC'".format(id))['rows'][0][0])
            result = cmg.format_response(True,data,'Data successfully processed!')
            logger.info("Data received from cache")
        except:
            logger.error("Error occurred while fetching data from Cache")
            result = cmg.format_response(False,None,'Error occurred while getting data from cache!',sys.exc_info())
            raise
        finally:
            return result