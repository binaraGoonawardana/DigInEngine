__author__ = 'Marlon'

import sys,os
sys.path.append("...")
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import descriptive_processor as dp
import ast
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']


def box_plot_generation(params, key, user_id, tenant):

        inputs = ast.literal_eval(params.q)
        dbtype = params.dbtype
        u_id = key

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            print err
            cache_timeout = int(default_cache_timeout)

        result = dp.ret_box(dbtype, inputs, u_id, cache_timeout, user_id, tenant)

        return result


def histogram_generation(params,key,user_id,tenant):
        inputs = ast.literal_eval(params.q)
        dbtype = params.dbtype
        n_bins = params.bins
        u_id = key

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            print err
            cache_timeout = int(default_cache_timeout)

        result = dp.ret_hist(dbtype, inputs, u_id, cache_timeout,n_bins,user_id, tenant)

        return result


def bubble_chart(params,key, user_id, tenant):

        dbtype = params.dbtype
        #db = params.db
        table = params.table
        u_id = key
        x = params.x
        y = params.y
        s = params.s
        c = params.c

        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            print err
            cache_timeout = int(default_cache_timeout)

        result = dp.ret_bubble(dbtype, table, x, y, s, c, u_id, cache_timeout, user_id, tenant)

        return result

