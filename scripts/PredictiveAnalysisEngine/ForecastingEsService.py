__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.1.0'

import sys, os
sys.path.append("...")
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import ForecastingEsProcessor as fes
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('CacheConfig.ini', 'Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']


def es_generation(params, key):


    dbtype = params.dbtype
    #db = params.db
    table = params.table
    u_id = key
    date = params.date_field
    f_field = params.f_field
    alpha = params.alpha
    beta = params.beta
    gamma = params.gamma
    n_predict = params.n_predict
    period = params.period
    len_season = params.len_season
    model = params.model
    method = params.method
    start_date = str(params.start_date)
    end_date = str(params.end_date)
    group_by = params.group_by

    try:
        cache_timeout = int(params.t)
    except AttributeError:

        cache_timeout = int(default_cache_timeout)

    result = fes.ret_exps(model, method, dbtype, table, u_id, date, f_field, alpha, beta, gamma, n_predict, period,
                          len_season, cache_timeout, start_date, end_date, group_by)

    return result
