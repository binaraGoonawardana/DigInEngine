__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import sys,os
sys.path.append("...")
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import algorithm_processor as ap
import ast
import configs.ConfigHandler as conf
import DiginAlgo as DA

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']

def linear_regression(params):
        dbtype = params.dbtype
        db = params.db
        table = params.table
        x = params.x
        y = params.y
        predict = ast.literal_eval(params.predict)

        result = DA.slr_get(dbtype, db,table, x, y, predict)
        return result

def kmeans_calculation(params,key):

        rec_data = ast.literal_eval(params.data)
        dbtype = params.dbtype
        id = key

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)
        # cache_timeout = 3
        # id = 'testing11'
        result = ap.ret_kmeans(dbtype, rec_data, id, cache_timeout)
        return result


