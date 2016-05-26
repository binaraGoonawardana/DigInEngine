__author__ = 'Marlon Abeykoon'
__version__ = '1.2.1.1'

import CommonFormulaeGenerator as cfg
import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as PG
import modules.CommonMessageGenerator as cmg
import scripts.DigINCacheEngine.CacheController as CC
import logging
import operator
import decimal
import json
import ast
import datetime
from multiprocessing import Process
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/AggregationEnhancer.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')

def MEMcache_insert(result,query, id, expiry):
            logger.info("Cache insertion started...")

            class ExtendedJSONEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, decimal.Decimal):
                        return str(obj)
                    if isinstance(obj, datetime) or isinstance(obj, datetime.date):
                        return obj.isoformat()
                    return super(ExtendedJSONEncoder, self).default(obj)

            createddatetime = datetime.datetime.now()
            expirydatetime = createddatetime + datetime.timedelta(seconds=expiry)
            # to_cache_lst = []
            # for k,v in result[0].iteritems():
            to_cache = { 'id': id,
                         # 'fieldname': k,
                         # 'value': v,
                         'data' : json.dumps(result, cls=ExtendedJSONEncoder),
                         'query' : str(query),
                         'expirydatetime': expirydatetime,
                         'createddatetime': createddatetime}
           # to_cache_lst.append(to_cache)
            try:
                CC.insert_data([to_cache],'cache_aggregation')
                logger.info("Cache insertion successful!")
            except Exception, err:
                logger.error("Error inserting to cache!")
                logger.error(err)
            finally:
                return None

def aggregate_fields(params, key):

        group_bys_dict = ast.literal_eval(params.group_by)  # {'a1':1,'b1':2,'c1':3}
        order_bys_dict = ast.literal_eval(params.order_by)  # {'a2':1,'b2':2,'c2':3}
        tablenames = ast.literal_eval(params.tablenames)  # 'tablenames' {1 : 'table1', 2:'table2', 3: 'table3'}
        aggregations = ast.literal_eval(params.agg) # [['field1' , 'sum'], ['field2' , 'avg']]
        conditions = params.cons # ''
        try:
            join_types = ast.literal_eval(params.joins) # {1 : 'left outer join', 2 : 'inner join'}
            join_keys = ast.literal_eval(params.join_keys) # {1: 'ON table1.field1' , 2: 'ON table2.field2'}
        except AttributeError,err:
            logger.info("Single table query received")
            join_types = {}
            join_keys = {}
            pass
        db = params.db
        dashboard_id = str(params.id)
        pkey = key
        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            logger.info("No cache timeout mentioned.")
            cache_timeout = int(default_cache_timeout)

        # SELECT a2, b2, c2, a1, b1, c1, sum(a3), sum(b3), sum(c3) FROM tablenames GROUP BY a1, b1, c1 ORDER BY a2, b2, c2
        time = datetime.datetime.now()
        try:
            cache_existance = CC.get_data("SELECT expirydatetime >= '{0}' FROM cache_aggregation WHERE id = '{1}'".format(time, pkey))['rows']
        except:
            logger.error("Error connecting to cache..")
            cache_existance = ()
            pass

        if len(cache_existance) == 0 or cache_existance[0][0] == 0 :

            if db.lower() == 'mssql':
                logger.info("MSSQL - Processing started!")
                query_body = tablenames[1]
                if join_types and join_keys != {}:
                    for i in range(0, len(join_types)):
                        sub_join_body = join_types[i+1] + ' ' + tablenames[i+2] + ' ' + join_keys[i+1]
                        query_body += ' '
                        query_body += sub_join_body

                if conditions:
                    conditions = 'WHERE %s' %(conditions)

                if group_bys_dict != {}:
                    logger.info("Group by statement creation started!")
                    grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))
                    group_bys_str = ''
                    group_bys_str_ = ''
                    if 1 in group_bys_dict.values():
                        group_bys = []
                        for i in range(0, len(grp_tup)):
                            group_bys.append(grp_tup[i][0])
                        group_bys_str_ = ', '.join(group_bys)
                        group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
                    logger.info("Group by statement creation completed!")

                else:
                    group_bys_str = ''
                    group_bys_str_ = ''

                if order_bys_dict != {}:
                    logger.info("Order by statement creation started!")
                    ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
                    order_bys_str = ''
                    order_bys_str_ = ''
                    if 1 in order_bys_dict.values():
                        Order_bys = []
                        for i in range(0, len(ordr_tup)):
                            Order_bys.append(ordr_tup[i][0])
                        order_bys_str_ = ', '.join(Order_bys)
                        order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                    logger.info("Order by statement creation completed!")

                else:
                    order_bys_str = ''

                logger.info("Select statement creation started!")
                aggregation_fields_set = []
                # for key, value in aggregations.iteritems(): # takes as a dictionary
                #     altered_field = key.replace('.','_')
                #     aggregation_fields = cfg.get_func('MSSQL',altered_field,value)
                #     #aggregation_fields = '{0}({1}) as {2}_{3}'.format(value, key, value, altered_field)
                #     aggregation_fields_set.append(aggregation_fields)

                for pair in aggregations:
                    altered_field = pair[0].replace('.','_') #['field1', 'sum']
                    aggregation_fields = cfg.get_func('MSSQL',altered_field,pair[1])
                    aggregation_fields_set.append(aggregation_fields)
                aggregation_fields_str = ', '.join(aggregation_fields_set)

                if 1 not in group_bys_dict.values() and 1 in order_bys_dict.values():
                    fields_list = [order_bys_str_, aggregation_fields_str]

                elif 1 not in order_bys_dict.values() and 1 in group_bys_dict.values():
                    fields_list = [group_bys_str_, aggregation_fields_str]

                elif 1 not in group_bys_dict.values() and 1 not in order_bys_dict.values():
                    fields_list = [aggregation_fields_str]

                else:
                    intersect_groups_orders = group_bys
                    intersect_groups_orders.extend(x for x in Order_bys if x not in intersect_groups_orders)
                    fields_list = intersect_groups_orders + [aggregation_fields_str]

                fields_str = ' ,'.join(fields_list)
                logger.info("Select statement creation completed!")
                query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str, query_body, conditions, group_bys_str,
                                                                 order_bys_str)
                print query
                logger.info('Query formed successfully! : %s' % query)
                logger.info('Fetching data from SQL...')
                result = ''

                try:
                    result_ = mssql.execute_query(query)
                    logger.info('Data received!')
                    p = Process(target=MEMcache_insert,args=(result_,query,pkey,cache_timeout))
                    p.start()
                    logger.debug('Result %s' % result)
                    logger.info("MSSQL - Processing completed!")
                    result = cmg.format_response(True,result_,query)
                except Exception, err:
                    logger.error('Error occurred while getting data from sql Handler!')
                    logger.error(err)
                    result = cmg.format_response(False,None,'Error occurred while getting data from sql Handler!',sys.exc_info())
                #result_dict = json.loads(result)
                finally:
                    return result

            elif db.lower() == 'bigquery':

                try:
                    agg_ = aggregations["Date, '%m'"]
                except:
                    agg_ = ''
                    pass
                if agg_ == 'STRFTIME_UTC_USEC':
                    query = "SELECT STRFTIME_UTC_USEC(Date, '%Y') as year, STRFTIME_UTC_USEC(Date, '%m') as month," \
                            " SUM(Sales) as sales, SUM(OrderQuantity) as tot_units FROM [Demo.forcast_superstoresales]" \
                            " GROUP BY year, month ORDER BY year, month"
                    result_ = BQ.execute_query(query)
                    result = cmg.format_response(True,result_,query)
                    return result
                else:
                    logger.info("BigQuery - Processing started!")
                    query_body = tablenames[1]
                    if join_types and join_keys != {}:
                        for i in range(0, len(join_types)):
                            sub_join_body = join_types[i+1] + ' ' + tablenames[i+2] + ' ' + join_keys[i+1]
                            query_body += ' '
                            query_body += sub_join_body

                    if conditions:
                        conditions = 'WHERE %s' %(conditions)

                    if group_bys_dict != {}:
                        logger.info("Group by statement creation started!")
                        grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))

                        group_bys_str = ''
                        group_bys_str_ = ''
                        if 1 in group_bys_dict.values():
                            group_bys = []
                            for i in range(0, len(grp_tup)):
                                group_bys.append(grp_tup[i][0])
                            group_bys_str_ = ', '.join(group_bys)
                            group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
                        logger.info("Group by statement creation completed!")
                    else:
                        group_bys_str = ''
                        group_bys_str_ = ''

                    if order_bys_dict != {}:
                        logger.info("Order by statement creation started!")
                        ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
                        order_bys_str = ''
                        order_bys_str_ = ''
                        if 1 in order_bys_dict.values():
                            Order_bys = []
                            for i in range(0, len(ordr_tup)):
                                Order_bys.append(ordr_tup[i][0])
                            order_bys_str_ = ', '.join(Order_bys)
                            order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                        logger.info("Order by statement creation completed!")

                    else:
                        order_bys_str = ''

                    logger.info("Select statement creation started!")
                    aggregation_fields_set = []
                    for pair in aggregations:
                        altered_field = pair[0].replace('.','_') #['field1', 'sum']
                        aggregation_fields = cfg.get_func('BigQuery',altered_field,pair[1])
                        aggregation_fields_set.append(aggregation_fields)
                    aggregation_fields_str = ', '.join(aggregation_fields_set)

                    if 1 not in group_bys_dict.values() and 1 in order_bys_dict.values():
                        fields_list = [order_bys_str_, aggregation_fields_str]

                    elif 1 not in order_bys_dict.values() and 1 in group_bys_dict.values():
                        fields_list = [group_bys_str_, aggregation_fields_str]

                    elif 1 not in group_bys_dict.values() and 1 not in order_bys_dict.values():
                        fields_list = [aggregation_fields_str]

                    else:
                        intersect_groups_orders = group_bys
                        intersect_groups_orders.extend(x for x in Order_bys if x not in intersect_groups_orders)
                        fields_list = intersect_groups_orders + [aggregation_fields_str]

                    fields_str = ' ,'.join(fields_list)

                    logger.info("Select statement creation started!")

                    query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str, query_body, conditions, group_bys_str,
                                                                     order_bys_str)
                    print query
                    logger.info('Query formed successfully! : %s' % query)
                    logger.info('Fetching data from SQL...')
                    result = ''

                    try:
                        result_ = BQ.execute_query(query)
                        result = cmg.format_response(True,result_,query,None)
                        logger.info('Data received!')
                        p = Process(target=MEMcache_insert,args=(result_,query,pkey,cache_timeout))
                        p.start()
                        logger.debug('Result %s' % result)
                        logger.info("BigQuery - Processing completed!")
                    except Exception, err:
                        logger.error('Error occurred while getting data from BQ Handler!')
                        logger.error(err)
                        result = cmg.format_response(False,None,'Error occurred while getting data from BQ Handler!',sys.exc_info())
                    #result_dict = json.loads(result)
                    finally:
                        return result

            elif db.lower() == 'postgresql':

                logger.info("Postgres - Processing started!")
                query_body = tablenames[1]
                if join_types and join_keys != {}:
                    for i in range(0, len(join_types)):
                        sub_join_body = join_types[i+1] + ' ' + tablenames[i+2] + ' ' + join_keys[i+1]
                        query_body += ' '
                        query_body += sub_join_body

                if conditions:
                    conditions = 'WHERE %s' %(conditions)

                if group_bys_dict != {}:
                    logger.info("Group by statement creation started!")
                    grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))

                    group_bys_str = ''
                    group_bys_str_ = ''
                    if 1 in group_bys_dict.values():
                        group_bys = []
                        for i in range(0, len(grp_tup)):
                            group_bys.append(grp_tup[i][0])
                        group_bys_str_ = ', '.join(group_bys)
                        group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
                    logger.info("Group by statement creation completed!")
                else:
                    group_bys_str = ''
                    group_bys_str_ = ''

                if order_bys_dict != {}:
                    logger.info("Order by statement creation started!")
                    ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
                    order_bys_str = ''
                    order_bys_str_ = ''
                    if 1 in order_bys_dict.values():
                        Order_bys = []
                        for i in range(0, len(ordr_tup)):
                            Order_bys.append(ordr_tup[i][0])
                        order_bys_str_ = ', '.join(Order_bys)
                        order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                    logger.info("Order by statement creation completed!")

                else:
                    order_bys_str = ''

                logger.info("Select statement creation started!")
                aggregation_fields_set = []
                for pair in aggregations:
                    altered_field = pair[0].replace('.','_') #['field1', 'sum']
                    aggregation_fields = cfg.get_func('postgresql',altered_field,pair[1])
                    aggregation_fields_set.append(aggregation_fields)
                aggregation_fields_str = ', '.join(aggregation_fields_set)

                if 1 not in group_bys_dict.values() and 1 in order_bys_dict.values():
                    fields_list = [order_bys_str_, aggregation_fields_str]

                elif 1 not in order_bys_dict.values() and 1 in group_bys_dict.values():
                    fields_list = [group_bys_str_, aggregation_fields_str]

                elif 1 not in group_bys_dict.values() and 1 not in order_bys_dict.values():
                    fields_list = [aggregation_fields_str]

                else:
                    intersect_groups_orders = group_bys
                    intersect_groups_orders.extend(x for x in Order_bys if x not in intersect_groups_orders)
                    fields_list = intersect_groups_orders + [aggregation_fields_str]

                fields_str = ' ,'.join(fields_list)

                logger.info("Select statement creation started!")

                query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str, query_body, conditions, group_bys_str,
                                                                 order_bys_str)
                print query
                logger.info('Query formed successfully! : %s' % query)
                logger.info('Fetching data from SQL...')
                result = ''

                try:
                    result_ = PG.execute_query(query)
                    result = cmg.format_response(True,result_,query,None)
                    logger.info('Data received!')
                    p = Process(target=MEMcache_insert,args=(result_,query,pkey,cache_timeout))
                    p.start()
                    logger.debug('Result %s' % result)
                    logger.info("PostgreSQL - Processing completed!")
                except Exception, err:
                    logger.error('Error occurred while getting data from PG Handler!')
                    logger.error(err)
                    result = cmg.format_response(False,None,'Error occurred while getting data from PG Handler!',sys.exc_info())
                # result_dict = json.loads(result)
                finally:
                    return result
        else:
            logger.info("Getting data from cache...")
            try:
                #cached_data = CC.get_data("SELECT fieldname, value FROM cache_aggregation WHERE dashboardid = '{0}'".format(dashboard_id))
                cached_data = CC.get_data("SELECT data, query FROM cache_aggregation WHERE id = '{0}'".format(pkey))
            except Exception, err:
                return cmg.format_response(False,None,'Error occurred while getting data from cache controller!',sys.exc_info())
            logger.info("Successful!")
            # dict = {}
            # for row in cached_data['rows']:
            #     d = {row[0]: float(row[1])}
            #     dict.update(d)
            print cached_data['rows']
            print cached_data['rows'][0][0]
            return cmg.format_response(True,json.loads(cached_data['rows'][0][0]),cached_data['rows'][0][1],None)


