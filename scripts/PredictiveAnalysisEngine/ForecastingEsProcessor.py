__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import sys
sys.path.append("...")
import pandas as pd
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as postgres
import modules.CommonMessageGenerator as cmg
import modules.TripleExponentialSmoothing as tes
import modules.DoubleExponentialSmoothing as des
import scripts.DigINCacheEngine.CacheController as CC
import configs.ConfigHandler as conf
import datetime
import logging
import json
import decimal
import numpy as np
from multiprocessing import Process

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/Forecasting_exponentialsmoothing.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('Starting log')

def es_getdata(dbtype, table, date, f_field, period):

    if dbtype.lower() == 'mssql':

        if period.lower() == 'daily':
            query = 'SELECT CAST({0} as DATE) date, SUM({1}) data from {2} GROUP BY CAST({0} as DATE)' \
                    ' Order by CAST({0} as DATE)'.format(date, f_field, table)
        elif period.lower() == 'monthly':
            query = 'SELECT DATEPART(yyyy,{0}) year, DATEPART(mm,{0}) month, SUM({1}) data from {2} ' \
                    'GROUP BY DATEPART(yyyy,{0}) , DATEPART(mm,{0}) ' \
                    'Order by DATEPART(yyyy,{0}) , DATEPART(mm,{0})'.format(date, f_field, table)
        elif period.lower() == 'yearly':
             query = 'SELECT DATEPART(yyyy,{0}) as date, sum({1}) as data FROM {2} GROUP BY DATEPART(yyyy,{0}) ' \
                     'ORDER BY DATEPART(yyyy,{0})'.format(date, f_field, table)
        try:
            result = mssql.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from MSSQL!', sys.exc_info())
            return result

    elif dbtype.lower() == 'bigquery':

        if period.lower() == 'daily':
            query = 'SELECT Date({0}) date, sum({1}) data FROM {2} GROUP BY Date ORDER BY Date'.\
                format(date, f_field, table)
        elif period.lower() == 'monthly':
            query = 'SELECT year({0}) year, month({0}) month, sum({1}) data FROM {2} GROUP BY year,month ' \
                    'ORDER BY year, month'.format(date, f_field, table)
        elif period.lower() == 'yearly':
             query = 'SELECT year({0}) date, sum({1}) data FROM {2} GROUP BY date ORDER BY date'.\
                 format(date, f_field, table)

        try:
            result = BQ.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from BigQuery Handler!',
                                         sys.exc_info())
            return result

    elif dbtype.lower() == 'postgresql':

        if period.lower() == 'daily':
            query = 'SELECT DATE::{0} as date, sum({1})::FLOAT as data FROM {2} GROUP BY {0} ORDER BY {0}'.\
                format(date, f_field, table)
        elif period.lower() == 'monthly':
            query = 'SELECT EXTRACT(year FROM {0}) as year, EXTRACT(month FROM {0}) as month, sum({1})::FLOAT as data ' \
                    'FROM {2} GROUP BY EXTRACT(year FROM {0}), EXTRACT(month FROM {0}) ' \
                    'ORDER BY EXTRACT(year FROM {0}), EXTRACT(month FROM {0})'.format(date, f_field, table)
        elif period.lower() == 'yearly':
             query = 'SELECT EXTRACT(year FROM {0}) as date, sum({1})::FLOAT as data FROM {2} ' \
                     'GROUP BY EXTRACT(year FROM {0}) ORDER BY EXTRACT(year FROM {0})'.format(date, f_field, table)

        try:
            result = postgres.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from Postgres Handler!',
                                         sys.exc_info())
            return result
    else:
        result = cmg.format_response(False, None, 'DB type not supported', sys.exc_info())

    return result

def cache_data(output, u_id, cache_timeout):

    logger.info("Cache insertion started...")
    createddatetime = datetime.datetime.now()
    expirydatetime = createddatetime + datetime.timedelta(seconds=cache_timeout)

    class ExtendedJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, decimal.Decimal):
                return str(obj)
            if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
                return obj.isoformat()
            if isinstance(obj, np.int64):
                return np.asscalar(np.int64(obj))
            return super(ExtendedJSONEncoder, self).default(obj)

    to_cache = [{'id': u_id, 'data': json.dumps(output, cls=ExtendedJSONEncoder), 'expirydatetime': expirydatetime,
                 'createddatetime': createddatetime}]

    try:
        p = Process(target=CC.insert_data,args=(to_cache, 'cache_forecasting'))
        logger.info('cache insertion is progressing')
        p.start()
    except Exception, err:
        logger.error(err, "Error inserting to cache!")


def ret_exps(model, method, dbtype, table, u_id, date, f_field, alpha, beta, gamma, n_predict, period,
             len_season, cache_timeout):

    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_forecasting WHERE id = '{1}'".
                                             format(time, u_id))['rows']

    except Exception, err:

        logger.error(err, "Error connecting to cache...")
        cache_existance = ()

    if len(cache_existance) == 0 or cache_existance[0][0] == 0:

        result = es_getdata(dbtype, table, date, f_field, period)

        try:
            df = pd.DataFrame(result)
            # df[['year', 'month']] = df[['year', 'month']].astype(str)
            # df['period'] = df[['year', 'month']].apply(lambda x: '-'.join(x), axis=1)

            series = df["data"].tolist()

            if model.lower() == 'triple_exp':
                if method.lower() == 'additive':
                    predicted = tes.triple_exponential_smoothing_additive(series, int(len_season), float(alpha),
                                                                          float(beta), float(gamma), int(n_predict))
                else:
                    predicted = tes.triple_exponential_smoothing_multiplicative(series, int(len_season), float(alpha),
                                                                                float(beta),float(gamma),int(n_predict))

            elif model.lower() == 'double_exp':
                if method.lower() == 'additive':
                    predicted = des.double_exponential_smoothing_additive(series, float(alpha), float(beta),
                                                                          int(n_predict))
                else:
                    predicted = des.double_exponential_smoothing_multiplicative(series, float(alpha), float(beta),
                                                                                int(n_predict))

            if period.lower() == 'daily' or period.lower() == 'yearly':
                output = {'actual': df['data'].tolist(), 'forecast': predicted, 'time': df['date'].tolist()}

            elif period.lower() == 'monthly':

                year = df["year"].tolist()
                month = df["month"].tolist()
                len_month = len(month)

                for i in range(1,int(n_predict)+1):
                    t = len_month+i
                    if int(month[-1]) == 12:
                        month.append(1)
                        yr = int(year[-1])+1
                        year.append(yr)
                    else:
                        mnth = int(month[-1])+1
                        month.append(mnth)
                        year.append(int(year[-1]))

                df2 = pd.DataFrame({'year': year,'month': month})
                df2[['year', 'month']] = df2[['year', 'month']].astype(str)
                df2['period'] = df2[['year', 'month']].apply(lambda x: '-'.join(x), axis=1)

                output = {'actual': df['data'].tolist(), 'forecast': predicted, 'time': df2['period'].tolist()}

            cache_data(output, u_id, cache_timeout)
            result = cmg.format_response(True, output, 'forecasting processed successfully!')

        except Exception, err:
            result = cmg.format_response(False,None,'Forecasting Failed!', sys.exc_info())

        finally:
            return result

    else:
        logger.info("Getting forecast data from Cache..")
        print 'recieved data from Cache'
        result = ''
        try:
            data = json.loads(CC.get_cached_data("SELECT data FROM cache_forecasting WHERE id = '{0}'".
                                                 format(u_id))['rows'][0][0])
            result = cmg.format_response(True, data, 'Data successfully received from cache!')
            logger.info("Data received from cache")
        except:
            logger.error("Error occurred while fetching data from Cache")
            result = cmg.format_response(False, None, 'Error occurred while getting data from cache!', sys.exc_info())
            raise
        finally:
            return result

