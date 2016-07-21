__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.2.0'

import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as postgres
import modules.CommonMessageGenerator as cmg
import modules.Bubblechart as Bubble
import modules.Histogram as Hist
import modules.Boxplot as Box
import pandas as pd
import scripts.DigINCacheEngine.CacheController as CC
import configs.ConfigHandler as conf
import datetime
import logging
import json
from multiprocessing import Process

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/DescriptiveAnalytics.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('Starting log')

def ret_data(dbtype, rec_data):

    df = pd.DataFrame()
    for i in range(0,len(rec_data)):
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

                result = cmg.format_response(False, err, 'Error occurred while getting data from MSSQL!', sys.exc_info())
                return result

        elif dbtype.lower() == 'bigquery':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
                result = BQ.execute_query(query)

            except Exception, err:

                result = cmg.format_response(False, err, 'Error occurred while getting data from BigQuery Handler!', sys.exc_info())
                return result

        elif dbtype.lower() == 'postgresql':

            try:
                query = 'SELECT {0}::FLOAT FROM {1}'.format(fields_str,tables_str)
                result = postgres.execute_query(query)

            except Exception, err:

                result = cmg.format_response(False, err, 'Error occurred while getting data from Postgres Handler!', sys.exc_info())
                return result

        if df.empty:
            df = pd.DataFrame(result)
        else:
            df1 = pd.DataFrame(result)
            df = pd.concat([df,df1],axis=1)

    return df

def cache_data(output, u_id, cache_timeout, c_name):

    logger.info("Cache insertion started...")
    createddatetime = datetime.datetime.now()
    expirydatetime = createddatetime + datetime.timedelta(seconds=cache_timeout)

    to_cache = [{'id': u_id, 'c_type': c_name, 'data': json.dumps(output), 'expirydatetime': expirydatetime, 'createddatetime': createddatetime}]

    try:
        p = Process(target=CC.insert_data,args=(to_cache, 'cache_descriptive_analytics'))
        logger.info('cache insertion is progressing')
        p.start()
    except Exception, err:
        logger.error("Error inserting to cache!")
        logger.error(err)

def ret_hist(dbtype, rec_data, u_id, cache_timeout,n_bins):

    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_descriptive_analytics"
                                             " WHERE id = '{1}' and c_type='histogram'".format(time, u_id))['rows']
        print 'recieved data from Cache'

    except Exception, err:
        print err
        logger.error("Error connecting to cache..")
        cache_existance = ()

    if len(cache_existance) == 0 or cache_existance[0][0] == 0:
        df = ret_data(dbtype, rec_data)

        try:
            output = Hist.histogram(df, n_bins)

            cache_data(output, u_id, cache_timeout, c_name='histogram')
            result = cmg.format_response(True, output, 'Histogram processed successfully!')

        except Exception, err:

            result = cmg.format_response(False, err, 'Histogram Failed!', sys.exc_info())

        finally:
            return result

    else:
        logger.info("Getting Histogram data from Cache..")
        result = ''
        try:
            data = json.loads(CC.get_cached_data("SELECT data FROM cache_descriptive_analytics WHERE id = '{0}' "
                                                 "and c_type='histogram'".format(u_id))['rows'][0][0])
            result = cmg.format_response(True, data, 'Data successfully processed!')
            logger.info("Data received from cache")
        except Exception:
            logger.error("Error occurred while fetching data from Cache")
            result = cmg.format_response(False, None, 'Error occurred while getting data from cache!', sys.exc_info())
            raise
        finally:
            return result

def ret_box(dbtype, rec_data, u_id, cache_timeout):

    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_descriptive_analytics "
                                             "WHERE id = '{1}' and c_type='boxplot'".format(time, u_id))['rows']
    except:
        logger.error("Error connecting to cache..")
        cache_existance = ()
        pass
    if len(cache_existance) == 0 or cache_existance[0][0] == 0:
        df = ret_data(dbtype, rec_data)

        try:
            output = Box.boxplot(df)
            cache_data(output, u_id, cache_timeout, c_name='boxplot')
            result = cmg.format_response(True, output, 'Boxplot processed successfully!')

        except Exception, err:

            result = cmg.format_response(False, err, 'Boxplot Failed!', sys.exc_info())

        finally:
            return result
    else:
        logger.info("Getting Histogram data from Cache..")
        result = ''
        try:
            data = json.loads(CC.get_cached_data("SELECT data FROM cache_descriptive_analytics "
                                                 "WHERE id = '{0}' and c_type='boxplot'".format(u_id))['rows'][0][0])
            result = cmg.format_response(True, data, 'Data successfully processed!')
            logger.info("Data received from cache")
        except:
            logger.error("Error occurred while fetching data from Cache")
            result = cmg.format_response(False, None, 'Error occurred while getting data from cache!', sys.exc_info())
            raise
        finally:
            return result

def ret_bubble(dbtype, table, x, y, s, c, u_id, cache_timeout):

    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_descriptive_analytics "
                                             "WHERE id = '{1}' and c_type='bubblechart'".format(time, u_id))['rows']
    except:
        logger.error("Error connecting to cache..")
        cache_existance = ()

    if len(cache_existance) == 0 or cache_existance[0][0] == 0:
        if dbtype.lower() == 'mssql':

            try:
                query = 'SELECT SUM("{1}") x, SUM("{2}") y, SUM("{3}") s, "{4}" c From {0} Group BY "{4}"'.\
                    format(table, x, y, s, c)
                result = mssql.execute_query(query)

            except Exception, err:
                result = cmg.format_response(False, err, 'Error occurred while getting data from MSSQL!',
                                             sys.exc_info())
                return result

        elif dbtype.lower() == 'bigquery':

            try:
                query = 'SELECT SUM({1}) x, SUM({2}) y, SUM({3}) s, {4} c From {0} Group BY c'.format(table, x, y, s, c)
                result = BQ.execute_query(query)

            except Exception, err:
                result = cmg.format_response(False, err, 'Error occurred while getting data from BigQuery Handler!',
                                             sys.exc_info())
                return result

        elif dbtype.lower() == 'postgresql':

            try:
                query = 'SELECT SUM({1})::FLOAT x, SUM({2})::FLOAT y, SUM({3})::FLOAT s, {4} c From {0} Group BY c'.\
                    format(table, x, y, s, c)
                result = postgres.execute_query(query)

            except Exception, err:
                result = cmg.format_response(False, err, 'Error occurred while getting data from Postgres Handler!',
                                             sys.exc_info())
                return result

        try:
            output = Bubble.bubblechart(result)
            cache_data(output, u_id, cache_timeout, c_name='bubblechart')
            result = cmg.format_response(True, output, 'Bubblechart processed successfully!')

        except Exception, err:
            result = cmg.format_response(False, err, 'Bubblechart Failed!', sys.exc_info())

        finally:
            return result
    else:
        logger.info("Getting Histogram data from Cache..")
        result = ''
        try:
            data = json.loads(CC.get_cached_data("SELECT data FROM cache_descriptive_analytics WHERE id = '{0}' "
                                                 "and c_type='bubblechart'".format(u_id))['rows'][0][0])
            result = cmg.format_response(True, data, 'Data successfully processed!')
            logger.info("Data received from cache")
        except:
            logger.error("Error occurred while fetching data from Cache")
            result = cmg.format_response(False, None, 'Error occurred while getting data from cache!',sys.exc_info())
            raise
        finally:
            return result
