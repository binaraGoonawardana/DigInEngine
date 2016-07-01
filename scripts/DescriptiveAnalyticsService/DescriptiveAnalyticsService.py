__author__ = 'Marlon'

import sys,os
sys.path.append("...")
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import descriptive_processor as dp
import web
import ast
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']


def box_plot_generation(params, key):
        # table_name = web.input().tablename
        # fields = ast.literal_eval(web.input().fields)
        # inputs = [{table_name:fields}]
        inputs = ast.literal_eval(params.q)
        dbtype = params.dbtype
        id = key

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)
        # try:
        result = dp.ret_box(dbtype, inputs, id, cache_timeout)
        #result_ = BP.ret_data(inputs)
            # result = cmg.format_response(True,result_,'Data successfully processed!')
        # except:
        #     logger.error("Error retrieving data from boxplot lib")
        #     result = cmg.format_response(False,None,'Error occurred while getting data from Box plot lib!',sys.exc_info())
        #     raise
        # finally:
        return result


def histogram_generation(params,key):
        inputs = ast.literal_eval(params.q)
        dbtype = params.dbtype
        n_bins = params.bins
        id = key

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)

        #try:
        result = dp.ret_hist(dbtype, inputs, id, cache_timeout,n_bins)
            #result = cmg.format_response(True,result_,'Data successfully processed!')
        # except:
        #     logger.error("Error retrieving data from histogram lib")
        #     result = cmg.format_response(False,None,'Error occurred while getting data from histogram lib!',sys.exc_info())
        #     raise
        # finally:
        return result

def bubble_chart(params,key):

        dbtype = params.dbtype
        #db = params.db
        table = params.table
        id = key
        x = params.x
        y = params.y
        s = params.s
        c = params.c

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)

        result = dp.ret_bubble(dbtype, table, x, y, s, c, id, cache_timeout)

        return result

