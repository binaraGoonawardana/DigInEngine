__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.1.0'

import sys
sys.path.append("...")
import pandas as pd
import datetime
import logging
import json
import decimal
import numpy as np
import threading
from multiprocessing import Process
from multiprocessing.dummy import Pool as tpool
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as postgres
import modules.CommonMessageGenerator as cmg
import modules.TripleExponentialSmoothing as tes
import modules.DoubleExponentialSmoothing as des
import scripts.DigINCacheEngine.CacheController as CC
import configs.ConfigHandler as conf

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
#http://localhost:8080/forecast?model=triple_exp&method=Additive&alpha=0.716&beta=0.029&gamma=0.993&n_predict=12&date_field=InvoiceDate&f_field=Sales&period=Monthly&len_season=12&start_date=%272015-01-11%27&end_date=%272015-10-01%27&group_by=&dbtype=BigQuery&table=[digin_duosoftware_com.sales_data]&SecurityToken=28f9b64148941e24ee65d3ac8cd32a06&Domain=digin.io


def es_getdata(dbtype, table, date, f_field, period, start_date, end_date, group, user_id, tenant, cat):

    if dbtype.lower() == 'bigquery':

        if start_date == '' and end_date == '':
            if group == '':
                where = ''
            else:
                where = 'WHERE '
                group = group.replace("AND", "")

        elif start_date == '' and end_date != '':
            where = ' WHERE Date({0}) <= {1}'.format(date, end_date)
        elif start_date != '' and end_date == '':
            where = ' WHERE Date({0}) >= {1}'.format(date, start_date)
        else:
            where = ' WHERE Date({0}) >= {1} AND Date({0}) <= {2}'.format(date, start_date, end_date)

        if period.lower() == 'daily':

            query = 'SELECT Date({0}) date, sum({1}) {4} FROM {2} {5} {3} GROUP BY Date ORDER BY Date'.\
                format(date, f_field, table, group, cat, where)

        elif period.lower() == 'monthly':

            query = 'SELECT year({0}) year, month({0}) month, sum({1}) {4} FROM {2} {5} {3} GROUP BY year,month ' \
                    'ORDER BY year, month'.format(date, f_field, table, group, cat, where)

        elif period.lower() == 'yearly':

            query = 'SELECT year({0}) date, sum({1}) {4} FROM {2} {5} {3} GROUP BY date ORDER BY date'.\
                format(date, f_field, table, group, cat, where)

        try:
            _df = BQ.execute_query(query, user_id= user_id, tenant= tenant)
            result = pd.DataFrame(_df)
        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from BigQuery Handler!',
                                         sys.exc_info())
        finally:
            return result

    elif dbtype.lower() == 'mssql':

        if start_date == '' and end_date == '':
            if group == '':
                where = ''
            else:
                where = 'WHERE '
                group = group.replace("AND", "")
        elif start_date == '' and end_date != '':
            where = ' WHERE CAST({0} as DATE) <= {1}'.format(date, end_date)
        elif start_date != '' and end_date == '':
            where = ' WHERE CAST({0} as DATE) >= {1}'.format(date, start_date)
        else:
            where = ' WHERE CAST({0} as DATE) >= {1} AND CAST({0} as DATE) >= {2}'.format(date, start_date, end_date)

        if period.lower() == 'daily':
            query = 'SELECT CAST({0} as DATE) date, SUM({1}) {4} from {2} {5} {3} GROUP BY CAST({0} as DATE)' \
                        ' Order by CAST({0} as DATE)'.format(date, f_field, table, group, cat, where)
        elif period.lower() == 'monthly':
            query = 'SELECT DATEPART(yyyy,{0}) year, DATEPART(mm,{0}) month, SUM({1}) {4} from {2} {5} {3} ' \
                        'GROUP BY DATEPART(yyyy,{0}) , DATEPART(mm,{0}) ' \
                        'Order by DATEPART(yyyy,{0}) , DATEPART(mm,{0})'.format(date, f_field, table, group, cat, where)
        elif period.lower() == 'yearly':
            query = 'SELECT DATEPART(yyyy,{0}) as date, sum({1}) as {4} FROM {2} {5} {3} GROUP BY DATEPART(yyyy,{0}) ' \
                         'ORDER BY DATEPART(yyyy,{0})'.format(date, f_field, table, group, cat, where)
        try:
            _df = mssql.execute_query(query)
            result = pd.DataFrame(_df)
        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from MSSQL!', sys.exc_info())

        finally:
            return result

    elif dbtype.lower() == 'postgresql':

        if start_date == '' and end_date == '':
            if group == '':
                where = ''
            else:
                where = 'WHERE '
                group = group.replace("AND", "")
        elif start_date == '' and end_date != '':
            where = ' WHERE DATE::{0} <= {1}'.format(date, end_date)
        elif start_date != '' and end_date == '':
            where = ' WHERE DATE::{0} >= {1}'.format(date, start_date)
        else:
            where = ' WHERE DATE::{0} >= {1} AND DATE::{0} >= {2}'.format(date, start_date, end_date)

        if period.lower() == 'daily':
            query = 'SELECT DATE::{0} as date, sum({1})::FLOAT as data FROM {2} GROUP BY {0} ORDER BY {0}'.\
                            format(date, f_field, table, group, cat, where)
        elif period.lower() == 'monthly':
            query = 'SELECT EXTRACT(year FROM {0}) as year, EXTRACT(month FROM {0}) as month, sum({1})::FLOAT as data ' \
                    'FROM {2} GROUP BY EXTRACT(year FROM {0}), EXTRACT(month FROM {0}) ORDER BY EXTRACT(year FROM {0}),' \
                    ' EXTRACT(month FROM {0})'.format(date, f_field, table, group, cat, where)
        elif period.lower() == 'yearly':
            query = 'SELECT EXTRACT(year FROM {0}) as date, sum({1})::FLOAT as data FROM {2} GROUP BY' \
                    'EXTRACT(year FROM {0}) ORDER BY EXTRACT(year FROM {0})'.\
                format(date, f_field, table, group, cat, where)

        try:
            _df = postgres.execute_query(query)
            result = pd.DataFrame(_df)

        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from Postgres Handler!',
                                                     sys.exc_info())
            return result

        finally:
            return result


def func_group(dbtype, table, group_by):
    if dbtype.lower() == 'bigquery':
        try:
            q = 'SELECT {0} FROM {1} GROUP BY {0}'.format(group_by, table)
            result = BQ.execute_query(q)
        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from BigQuery Handler!',
                                         sys.exc_info())
        finally:
            return result

    elif dbtype.lower() == 'mssql':
        try:
            q = 'SELECT DISTINCT {0} FROM {1}'.format(group_by, table)
            result = mssql.execute_query(q)

        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from MSSQL!', sys.exc_info())

        finally:
            return result

    elif dbtype.lower() == 'postgresql':
        try:
            q = 'SELECT DISTINCT {0} FROM {1}'.format(group_by, table)
            result = postgres.execute_query(q)
        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from Postgres Handler!',
                                                     sys.exc_info())
        finally:
            return result


def _exist_date_range(dbtype, table,  start_date, end_date):

    if dbtype.lower() == 'bigquery':
        try:
            q = 'SELECT count(*) FROM {0} WHERE  {0}'.format(table, start_date, end_date)
            result = BQ.execute_query(q)
            return result
        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from BigQuery Handler!',
                                         sys.exc_info())
        finally:
            return result

    elif dbtype.lower() == 'mssql':
        try:
            q = 'SELECT DISTINCT {0} FROM {1}'.format(table, start_date, end_date)
            result = mssql.execute_query(q)
            return result

        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from MSSQL!', sys.exc_info())

        finally:
            return result

    elif dbtype.lower() == 'postgresql':
        try:
            q = 'SELECT DISTINCT {0} FROM {1}'.format(table, start_date, end_date)
            result = postgres.execute_query(q)
            return result
        except Exception, err:
            result = cmg.format_response(False, err, 'Error occurred while getting data from Postgres Handler!',
                                                     sys.exc_info())
        finally:
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


def _date(df, period, n_predict, dates):

    if period.lower() == 'daily':
        dt = df['date'].tolist()
        for _ in range(int(n_predict)):
            k = datetime.datetime.strptime(dt[-1], '%Y-%m-%d').date() + datetime.timedelta(days=1)
            dt.append(str(k))

    elif period.lower() == 'yearly':
        dt = df["date"].tolist()
        for _ in range(int(n_predict)):
            yr = int(dt[-1])+1
            dt.append(yr)

    #SELECT to_char(current_date, 'yyyy-MON') - postgres
    #SELECT CONVERT(CHAR(3), DATENAME(MONTH, GETDATE())) - MSSQL
    #STRFTIME_UTC_USEC(InvoiceDate,'%Y-%b') - Bigquery
    elif period.lower() == 'monthly':

        year = df["year"].tolist()
        month = df["month"].tolist()

        for _ in range(1, int(n_predict)+1):

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
            dt = df2['period'].tolist()
    #dates.append(dt)
    return dt


def _forecast(model, method, series, len_season, alpha, beta, gamma, n_predict, predicted):
    if model.lower() == 'triple_exp':
        if method.lower() == 'additive':
            pred = tes.triple_exponential_smoothing_additive(series, int(len_season), float(alpha),
                                                                  float(beta), float(gamma), int(n_predict))
        else:
            pred = tes.triple_exponential_smoothing_multiplicative(series, int(len_season), float(alpha),
                                                                        float(beta),float(gamma),int(n_predict))

    elif model.lower() == 'double_exp':
        if method.lower() == 'additive':
            pred = des.double_exponential_smoothing_additive(series, float(alpha), float(beta), int(n_predict))
        else:
            pred = des.double_exponential_smoothing_multiplicative(series, float(alpha), float(beta), int(n_predict))

    #predicted.append(pred)
    return pred


def ret_exps(model, method, dbtype, table, u_id, date, f_field, alpha, beta, gamma, n_predict, period,
             len_season, cache_timeout, start_date, end_date, group_by, user_id, tenant):

    time = datetime.datetime.now()
    try:
        cache_existence = CC.get_cached_data("SELECT expirydatetime >= '{0}' FROM cache_forecasting WHERE id = '{1}'".
                                             format(time, u_id))['rows']
    except Exception, err:
        logger.error(err, "Error connecting to cache...")
        cache_existence = ()

    if len(cache_existence) == 0 or cache_existence[0][0] == 0:
        try:

            dates = []
            predicted = []
            if group_by == '':
                result = es_getdata(dbtype, table, date, f_field, period, start_date, end_date, group_by, user_id,
                                    tenant, cat='data')
                df = pd.DataFrame(result)
                series = df["data"].tolist()
                predicted = _forecast(model, method, series, len_season, alpha, beta, gamma, n_predict, predicted)
                dates = _date(df, period, n_predict, dates)
                output = {'data': {'actual': df['data'].tolist(), 'forecast': predicted, 'time': dates}}

            else:
                group_dic = func_group(dbtype, table, group_by)
                group_ls = [(i.values()[0]) for i in group_dic]

                d = {}
                for cat in group_ls:
                    group = ' AND {0} = "{1}"'.format(group_by, cat)
                    result = es_getdata(dbtype, table, date, f_field, period,  start_date, end_date, group, user_id,
                                        tenant, cat.replace(" ", ""))
                    d[cat] = pd.DataFrame(result)
                output = {}
                #merging dataframes dynamically with full outer join
                if period.lower() == 'monthly':
                    df = reduce(lambda left, right: pd.merge(left, right, on=['year', 'month'], how='outer'),
                                    d.values())
                    df = df.fillna(0)
                    for i in range(len(df.columns)):
                        if i == df.columns.get_loc('year'):
                            continue
                        elif i == df.columns.get_loc('month'):
                            continue
                        else:
                            series = df.ix[:, i]
                            col_n = df.columns[i]
                            predicted = _forecast(model, method, series, len_season, alpha, beta, gamma, n_predict,
                                                  predicted)
                            dates = _date(df, period, n_predict, dates)
                            output[col_n] = {'actual': df[col_n].tolist(), 'forecast': predicted, 'time': dates}
                else:
                    df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), d.values())
                    df = df.fillna(0)
                    for i in range(len(df.columns)):
                        if i == df.columns.get_loc('date'):
                            continue
                        else:
                            series = df.ix[:, i]
                            col_n = df.columns[i]
                            predicted = _forecast(model, method, series, len_season, alpha, beta, gamma, n_predict,
                                                  predicted)
                            dates = _date(df, period, n_predict, dates)
                            output[col_n] = {'actual': df[col_n].tolist(), 'forecast': predicted, 'time': dates}

            cache_data(output, u_id, cache_timeout)
            result = cmg.format_response(True, output, 'forecasting processed successfully!')

        except Exception, err:
            result = cmg.format_response(False, err, 'Forecasting Failed!', sys.exc_info())

        finally:
            return result

    else:
        logger.info("Getting forecast data from Cache..")
        print 'received data from Cache'
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
