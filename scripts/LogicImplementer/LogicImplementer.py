__author__ = 'Marlon Abeykoon'
__version__ = '1.1.1'

import sys,os
 #code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)

import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as PG
import scripts.DigINCacheEngine.CacheController as CC
import modules.CommonMessageGenerator as cmg
from multiprocessing import Process
import json
import operator
import ast
import logging
import datetime
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/LogicImplementer.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  LogicImplementer  -----------------------------------------------')
logger.info('Starting log')


#http://localhost:8080/hierarchicalsummary?h={%22vehicle_usage%22:1,%22vehicle_type%22:2,%22vehicle_class%22:3}&tablename=[digin_hnb.hnb_claims]&conditions=date%20=%20%272015-05-04%27%20and%20name=%27marlon%27&id=1
def create_hierarchical_summary(params, cache_key):

        table_name = params.tablename
        dictb = ast.literal_eval(params.h)
        db = params.db
        ID = str(params.id)
        pkey = cache_key
        # dictb = {"vehicle_usage":1,"vehicle_type":2,"vehicle_class":3}
        tup = sorted(dictb.items(), key=operator.itemgetter(1))
        where_clause = ''
        try:
            conditions = params.conditions
            where_clause = 'WHERE %s' % conditions
        except:
            pass
        try:
            cache_timeout = int(params.t)
        except AttributeError, err:
            logger.info("No cache timeout mentioned.")
            cache_timeout = int(default_cache_timeout)
        logger.info('Requested received: Keys: {0}, values: {1}'.format(params.keys(),params.values()))

        fields = []  # ['aaaa', 'bbbb', 'cccc']
        counted_fields = []
        partition_by = []
        count_statement = []
        window_functions_set = []

        # cache_state = None
        # try:
        #     cache_state = CC.get_data('Hierarchy_summary','is_expired','ID=%s'%ID).is_expired
        # except:
        #     logger.info("No data in cache")
        #     pass
        time = datetime.datetime.now()
        try:
            cache_existance = CC.get_data("SELECT expirydatetime >= '{0}' FROM cache_hierarchy_summary WHERE id = '{1}'".format(time, pkey))['rows']
        except:
            logger.error("Error connecting to cache..")
            cache_existance = ()
            pass

        if len(cache_existance) == 0 or cache_existance[0][0] == 0 :
            for i in range(0, len(tup)):
                fields.append(tup[i][0])
                counted_fields.append('%s_count' % (tup[i][0]))  # ['aaaa_count', 'bbbb_count', 'cccc_count']
                p = []
                count_statement.append('COUNT (%s) as %s_count' % (tup[i][0], tup[i][0]))
                for j in range(0, i+1):
                    p.append(fields[j])
                p_str = ', '.join(p)
                partition_by.append(p_str)
                window_functions = 'SUM(%s) OVER (PARTITION BY %s) as %s_count1' % \
                                   (counted_fields[i], str(partition_by[i]), tup[i][0])
                # SUM(cccc_count) OVER (PARTITION BY ['aaaa', 'bbbb', 'cccc']) as cccc_count1
                window_functions_set.append(window_functions)

            total_str = 'SUM(%s_count) OVER () as total' % (tup[0][0])
            fields_str = ', '.join(fields)
            window_functions_set_str = ', '.join(window_functions_set)
            count_statement_str = ', '.join(count_statement)

            query = 'SELECT {0},{1}, {2} FROM (SELECT {3}, {4} FROM {5} {6} GROUP BY {7} )z ORDER BY {8}'\
                .format(fields_str, total_str, window_functions_set_str, fields_str, count_statement_str, table_name,
                        where_clause, fields_str, fields_str)
            logger.info('Query formed successfully! : %s' % query)
            logger.info('Fetching data from BigQuery...')
            result = ''
            if db.lower() == 'bigquery':
                try:
                    result = BQ.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from BigQuery Handler! %s' % err)
                    return cmg.format_response(False,None,'Error occurred while getting data from BigQuery Handler!',sys.exc_info())

            elif db.lower() == 'mssql':
                try:
                    result = mssql.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from sql Handler! %s' % err)
                    return cmg.format_response(False,None,'Error occurred while getting data from BigQuery Handler!',sys.exc_info())

            elif db.lower() == 'postgresql':
                try:
                    result = PG.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from pgsql Handler! %s' % err)
                    return cmg.format_response(False,None,'Error occurred while getting data from Postgres Handler!',sys.exc_info())

            result_dict = result
            #  sets up json
            #  levels_memory = {'vehicle_usage': [], 'vehicle_type': [], 'vehicle_class': []}
            total = result_dict[0]["total"]
            levels_memory_f = []
            levels_memory_str = '{%s}'
            for i in range(0, len(fields)):
                levels_memory_f.append("'{0}': []".format(fields[i]))
            levels_index = dict(zip(dictb.values(), dictb.keys()))
            result = []

            logger.info('Started building hierarchy.')

            def build_node(obj, key):
                '''This build the node for your result list'''
                return {
                    'name': obj[key],
                    'imageURL': '',
                    'type': obj[key],
                    'size': obj['%s_count1' % key],
                    'children': []
                }

            def build_level(input_list, keyindex):
                ''' This build one level at a time but call itself recursively'''
                key = levels_index[keyindex]
                levels_memory = ast.literal_eval(levels_memory_str % ' ,'.join(levels_memory_f))
                output = []
                for obj in input_list:
                    if obj[key] not in levels_memory[key]:
                        levels_memory[key].append(obj[key])
                        output.append(build_node(obj, key))
                        if keyindex < len(levels_index):
                            output[-1]['children'] = build_level(
                                [_ for _ in input_list if _[key] == output[-1]['name']],
                                keyindex + 1)
                return output
            children_list = build_level(result_dict, 1)
            final_result = {"name": "TOTAL",
                            "imageURL": "",
                            "type": "TOTAL",
                            "size": total,
                            "children": children_list}
            logger.debug("Final result %s" % final_result)

            logger.info('Data processed successfully...')
            # try:
            #     CC.insert_data([{'ID': ID, 'createddatetime': str(datetime.datetime.now()),
            #                      'data': json.dumps(final_result), 'is_expired': 0}], 'Hierarchy_summary')
            #     logger.info("Cache Update Successful")
            # except Exception, err:
            #     logger.error("Error in updating cache. %s" % err)
            #     pass
            logger.info("Cache insertion started...")
            createddatetime = datetime.datetime.now()
            expirydatetime = createddatetime + datetime.timedelta(seconds=cache_timeout)

            to_cache = [{'id': pkey,
                         'data': json.dumps(final_result),
                         'expirydatetime': expirydatetime,
                         'createddatetime': createddatetime}]
            try:
                p = Process(target=CC.insert_data,args=(to_cache,'cache_hierarchy_summary'))
                p.start()
            except Exception, err:
                logger.error("Error inserting to cache!")
                logger.error(err)
                pass

            return cmg.format_response(True,final_result,'Data successfully processed!')

        else:
            logger.info("Getting Hierarchy_summary data from Cache..")
            result = ''
            try:
                data = json.loads(CC.get_data("SELECT data FROM cache_hierarchy_summary WHERE id = '{0}'".format(pkey))['rows'][0][0])
                print data
                result = cmg.format_response(True,data,'Data successfully processed!')
                logger.info("Data received from cache")
            except:
                logger.error("Error occurred while fetching data from Cache")
                result = cmg.format_response(False,None,'Error occurred while getting data from cache!',sys.exc_info())
                raise
            finally:
                return result


def MEM_insert(data,cache_timeout):
        logger.info("Cache insertion started...")
        createddatetime = datetime.datetime.now()
        expirydatetime = createddatetime + datetime.timedelta(seconds=cache_timeout)

        to_cache_lst = []
        for row in data:
            to_cache = {'id': str(row['ID']),
                         'level': row['level'],
                         'value': row['value'],
                         'expirydatetime': expirydatetime,
                         'createddatetime': createddatetime}
            to_cache_lst.append(to_cache)
        try:
            CC.insert_data(to_cache_lst,'cache_hierarchy_levels')

        except Exception, err:
            logger.error("Error inserting to cache!")
            logger.error(err)
            pass

def get_highest_level(params, cache_key):
    logging.info("Entered getHighestLevel.")
    table_name = params.tablename
    ID = str(params.id)
    pkey = cache_key
    levels = [item.encode('ascii') for item in ast.literal_eval(params.levels)]
    db = params.db
    try:
        previous_lvl = params.plvl
    except:
        previous_lvl = ''   # If plvl is not sent assign an empty string
    where_clause = ''
    try:
        conditions = params.conditions
        where_clause = 'WHERE %s' % conditions
    except:
        pass

    try:
        cache_timeout = int(params.t)
    except AttributeError, err:
        logger.info("No cache timeout mentioned.")
        cache_timeout = int(default_cache_timeout)

    #check_result = CC.get_data(('Hierarchy_table','value',conditions))
    time = datetime.datetime.now()
    try:
        cache_existance = CC.get_data("SELECT expirydatetime >= '{0}' FROM cache_hierarchy_levels WHERE id = '{1}'".format(time, pkey))['rows']
    except Exception, err:
        logger.error("Error connecting to cache..")
        logger.error(err)
        cache_existance = ()
        pass

    if len(cache_existance) == 0 or cache_existance[0][0] == 0 :
        #if len(previous_lvl) == 0 or previous_lvl == 'All':
            # If plvl is not sent going to create the hierarchy assuming the data is not there in MEMSQL
            if db.lower() == 'bigquery':
                query = 'select count(level) as count, level from  {0}  group by level'
                sub_body = []
                for i in range(0,len(levels)):
                    sub_body.append('(select {0}, "{1}" as level from {2} {3} group by {4})'
                                    .format(levels[i],levels[i],table_name,where_clause,levels[i]))
                sub_body_str = ' ,'.join(sub_body)
                query = query.format(sub_body_str)  # UNION is not supported in BigQuery
                logger.info("Query formed! %s" % query )
                logger.info("Fetching data from BigQuery..")
                result = ''
                try:
                    result = BQ.execute_query(query)
                    # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"},
                    # {"count": 8, "level": "vehicle_class"}]
                    logger.info("Data received!")
                    logger.debug("result %s" %result)
                except Exception, err:
                    logger.error('Error occurred while getting data from BigQuery Handler! %s' % err)
                    return cmg.format_response(False,None,'Error occurred while getting data from BigQuery Handler!',sys.exc_info())
                sorted_x = sorted(result, key=lambda k: k['count'])
                # Sort the dict to get the form the hierarchy (tuple is formed)
                hi_list = []  # This will contain the dictionary list ready to insert to MEMSql

                for i in range(0, len(sorted_x)):
                    dicth = {}
                    dicth['ID'] = pkey
                    dicth['level'] = i+1
                    dicth['value'] = sorted_x[i]['level']
                    hi_list.append(dicth)
                try:
                    logger.info('Inserting to cache..')
                    p = Process(target=MEM_insert,args=(hi_list,cache_timeout))
                    p.start()
                except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
                if previous_lvl == 'All':
                    return cmg.format_response(True,hi_list,'Data successfully processed!')
                else:
                    return cmg.format_response(True,sorted_x[0]['level'],'Data successfully processed!')


            elif db.lower() == 'mssql':
                levels_ = levels
                query = 'select count(level) as count, level from  ( {0} )a group by level'

                if " " in table_name:
                    table_name = '['+table_name+']'
                    print table_name

                for field in levels_:
                    if " " in field:
                        levels = []
                for field in levels_:
                    if " " in field:
                        field = '['+field+']'
                        levels.append(field)

                sub_body = []
                for i in range(0,len(levels)):
                    sub_body.append("(select  convert(varchar(30), {0}, 1) as ee, '{1}' as level from {2} {3} group by {4})"
                                    .format(levels[i],levels[i],table_name,where_clause,levels[i]))
                sub_body_str = ' union '.join(sub_body)
                query = query.format(sub_body_str)  # UNION is not supported in BigQuery
                logger.info("Query formed! %s" % query )
                logger.info("Fetching data from BigQuery..")
                result = ''
                try:
                    result = mssql.execute_query(query)
                    # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"},
                    # {"count": 8, "level": "vehicle_class"}]
                    logger.info("Data received!")
                    logger.debug("result %s" %result)
                except Exception, err:
                    logger.error('Error occurred while getting data from SQL Handler! %s' % err)
                    return cmg.format_response(False,None,'Error occurred while getting data from BigQuery Handler!',sys.exc_info())

                sorted_x = sorted(result, key=lambda k: k['count'])
                # Sort the dict to get the form the hierarchy (tuple is formed)
                hi_list = []  # This will contain the dictionary list ready to insert to MEMSql

                for i in range(0, len(sorted_x)):
                    dicth = {}
                    dicth['ID'] = pkey
                    dicth['level'] = i+1
                    dicth['value'] = sorted_x[i]['level']
                    hi_list.append(dicth)
                try:
                    logger.info('Inserting to cache..')
                    p = Process(target=MEM_insert,args=(hi_list,cache_timeout))
                    p.start()
                except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
                if previous_lvl == 'All':
                    return cmg.format_response(True,hi_list,'Data successfully processed!')
                else:
                    return cmg.format_response(True,sorted_x[0]['level'],'Data successfully processed!')

            elif db.lower() == 'postgresql': #TODO DO unit testing
                levels_ = levels
                query = 'select count(level) as count, level from  ( {0} )a group by level'

                sub_body = []
                for i in range(0,len(levels)):
                    sub_body.append("(select  convert(varchar(30), {0}, 1) as ee, '{1}' as level from {2} {3} group by {4})"
                                    .format(levels[i],levels[i],table_name,where_clause,levels[i]))
                sub_body_str = ' union '.join(sub_body)
                query = query.format(sub_body_str)  # UNION is not supported in BigQuery
                logger.info("Query formed! %s" % query )
                logger.info("Fetching data from BigQuery..")
                result = ''
                try:
                    result = PG.execute_query(query)
                    # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"},
                    # {"count": 8, "level": "vehicle_class"}]
                    logger.info("Data received!")
                    logger.debug("result %s" %result)
                except Exception, err:
                    logger.error('Error occurred while getting data from SQL Handler! %s' % err)
                    return cmg.format_response(False,None,'Error occurred while getting data from BigQuery Handler!',sys.exc_info())

                sorted_x = sorted(result, key=lambda k: k['count'])
                # Sort the dict to get the form the hierarchy (tuple is formed)
                hi_list = []  # This will contain the dictionary list ready to insert to MEMSql

                for i in range(0, len(sorted_x)):
                    dicth = {}
                    dicth['ID'] = pkey
                    dicth['level'] = i+1
                    dicth['value'] = sorted_x[i]['level']
                    hi_list.append(dicth)
                try:
                    logger.info('Inserting to cache..')
                    p = Process(target=MEM_insert,args=(hi_list,cache_timeout))
                    p.start()
                except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                    pass
                if previous_lvl == 'All':
                    return cmg.format_response(True,hi_list,'Data successfully processed!')
                else:
                    return cmg.format_response(True,sorted_x[0]['level'],'Data successfully processed!')


    else:
        if len(previous_lvl) == 0 or previous_lvl == 'All':
            data = CC.get_data("SELECT id, level, value FROM cache_hierarchy_levels WHERE id = '{0}'".format(pkey))['rows']
            dict_lst = []
            for row in data:
                dict = {'ID': row[0],
                        'value': row[2],
                        'level': row[1]}
                dict_lst.append(dict)
            return cmg.format_response(True,dict_lst,'Data successfully processed!')
        else:
            logger.info("Getting data from cache..")
            data = CC.get_data("SELECT id, level, value FROM cache_hierarchy_levels WHERE id = '{0}' and  level = {1}".format(pkey,int(previous_lvl)+1))['rows']
            logger.info("Data received from cache!")
            try:
                dict = {'ID': data[0][0],
                        'value': data[0][2],
                        'level': data[0][1]}
                logger.info("Returned: %s" % dict )
                return cmg.format_response(True,dict,'Data successfully processed!')
            except:
                logger.warning("Nothing to return, End of hierarchy!")
                return cmg.format_response(False,None,'End of hierarchy',sys.exc_info())

