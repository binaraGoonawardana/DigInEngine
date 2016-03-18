__author__ = 'Sajeetharan'

import json
import sys
import os,sys,inspect
import web
import json
import operator
import ast
import logging
import datetime
from sqlalchemy import text
import os, sys
import json
import web
import pyodbc

import CommonFormulaeGenerator as cfg
import sqlalchemy as sql
from pandas import DataFrame
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
from bigquery import get_client
import  BigQueryHandler as BQ
import ForecastingProcessor as FP
import SQLQueryHandler as mssql
try:
    import  CacheController as CC
except:
    pass
import test_sqlsplit as splitter
import json
import web
import urllib
from bigquery import get_client
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('LogicImplementer.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
query = ""
project_id = 'thematic-scope-112013'
service_account = 'diginowner@thematic-scope-112013.iam.gserviceaccount.com'
key = 'Digin-f537471c3b66.p12'
engine = sql.create_engine("mssql+pyodbc://smsuser:sms@192.168.1.83:1433/Demo?driver=SQL+Server+Native+Client+11.0")

metadata = sql.MetaData()
connection = engine.connect()
urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables',
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields',
    '/refinegeoloc(.*)','RefineGeoLocations',
     '/forecast(.*)', 'Forecasting_1',
    '/gethistoricalgroupeddata(.*)', 'GetHistoricalGroupedData'
)
app = web.application(urls, globals())
class execute_query:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          query = web.input().query
          db = web.input().db
          columns = []
          if db == 'BigQuery':
             client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=False)
             job_id, _results = client.query(query)
             complete, row_count = client.check_job(job_id)
             results = client.get_query_rows(job_id)
             return  json.dumps(results)
          elif db == 'MSSQL':
              data = []
              sql = text(query)
              result = connection.execute(sql)
              columns = result.keys()
              print columns
              results = []
              for row in result:
                results.append(dict(zip(columns, row)))
              return json.dumps(results)
          else:
              return "db not implemented"

class get_Fields:
   def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          fields = []
          datasetname = web.input().dataSetName
          tablename = web.input().tableName
          db = web.input().db
          if db == 'BigQuery':
            client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
            results = client.get_table_schema(datasetname,tablename)
            for x in results:
             fields.append(x['name'])
            return json.dumps(fields)
          elif db == 'MSSQL':
               fields = []
               datasetname = web.input().dataSetName
               tablename = web.input().tableName
               query = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+tablename + "'";
               sql = text(query)
               result = connection.execute(sql)
               for row in result:
                 fieldtype = {'Fieldname': row[3],
                        'FieldType':row[7]}
                 fields.append(fieldtype)
               return json.dumps(fields)
          else:
              return "db not implemented"


class get_Tables:
    def GET(self,r):
          web.header('Access-Control-Allow-Origin',      '*')
          web.header('Access-Control-Allow-Credentials', 'true')
          tables = []
          datasetID = web.input().dataSetName
          db = web.input().db
          if db == 'BigQuery':
           client = get_client(project_id, service_account=service_account,
                            private_key_file=key, readonly=True)
           result  = client._get_all_tables(datasetID,cache=False)
           tablesWithDetails =    result["tables"]
           for inditable in tablesWithDetails:
              tables.append(inditable["id"])
           print(json.dumps(tables))
           tables = [i.split('.')[-1] for i in tables]
           return json.dumps(tables)
          elif db == 'MSSQL':
           tables = []
           datasetID = web.input().dataSetName
           #connection_string = 'DRIVER={SQL Server};SERVER=192.168.1.83;DATABASE='+ datasetID +';UID=smsuser;PWD=sms';
           query = "SELECT * FROM information_schema.tables"
           result = connection.execute(query)
           for row in result:
              tables.append(row[2])
           return json.dumps(tables)
          else:
              return "db not implemented"



class createHierarchicalSummary(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        table_name = web.input().tablename
        dictb = ast.literal_eval(web.input().h)
        db = web.input().db
        ID = int(web.input().id)
        # dictb = {"vehicle_usage":1,"vehicle_type":2,"vehicle_class":3}
        tup = sorted(dictb.items(), key=operator.itemgetter(1))
        where_clause = ''
        try:
            conditions = web.input().conditions
            where_clause = 'WHERE %s' % conditions
        except:
            pass
        logger.info('Requested received: %s' % web.input().values())
        fields = []  # ['aaaa', 'bbbb', 'cccc']
        counted_fields = []
        partition_by = []
        count_statement = []
        window_functions_set = []

        cache_state = None
        try:
            cache_state = CC.get_data('Hierarchy_summary','is_expired','ID=%s'%ID).is_expired
        except:
            logger.info("No data in cache")

        if cache_state == 1 or cache_state is None:
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
            if db == 'BigQuery':
                try:
                    result = BQ.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from BigQuery Handler! %s' % err)
                    raise
            elif db == 'MSSQL':
                try:
                    result = mssql.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from sql Handler! %s' % err)
                    raise

            result_dict = json.loads(result)
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

            final_result_json = json.dumps(final_result)
            logger.info('Data processed successfully...')
            try:
                CC.insert_data([{'ID': ID, 'createddatetime': str(datetime.datetime.now()),
                                 'data': final_result_json, 'is_expired': 0}], 'Hierarchy_summary')
                logger.info("Cache Update Successful")
            except Exception, err:
                logger.error("Error in updating cache. %s" % err)
                pass

            return final_result_json
        else:
            logger.info("Getting Hierarchy_summary data from Cache..")
            try:
                data = CC.get_data('Hierarchy_summary', 'data', 'ID=%s ' % ID).data
                logger.info("Data received from cache")
                return json.dumps(json.loads(data))
            except:
                logger.error("Error occurred while fetching data from Cache")
                return "Error"

#http://localhost:8080/gethighestlevel?tablename=[digin_hnb.hnb_claims]&id=1&levels=['vehicle_usage','vehicle_class','vehicle_type']&plvl=All
class getHighestLevel(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        logging.info("Entered getHighestLevel.")
        table_name = web.input().tablename
        ID = web.input().id
        levels = [item.encode('ascii') for item in ast.literal_eval(web.input().levels)]
        db = web.input().db
        try:
            previous_lvl = web.input().plvl
        except:
            previous_lvl = ''   # If plvl is not sent assign an empty string
        where_clause = ''
        try:
            conditions = web.input().conditions
            where_clause = 'WHERE %s' % conditions
        except:
            pass

        logger.info('Request received: %s' % web.input().values())

        #  check_result = CC.get_data(('Hierarchy_table','value',conditions))
        if len(previous_lvl) == 0 or previous_lvl == 'All':
        # If plvl is not sent going to create the hierarchy assuming the data is not there in MEMSQL
            if db == 'BigQuery':
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
                    result = json.loads(BQ.execute_query(query))
                    # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"},
                    # {"count": 8, "level": "vehicle_class"}]
                    logger.info("Data received!")
                    logger.debug("result %s" %result)
                except Exception, err:
                    logger.error('Error occurred while getting data from BigQuery Handler! %s' % err)
                    raise
                sorted_x = sorted(result, key=lambda k: k['count'])
                # Sort the dict to get the form the hierarchy (tuple is formed)
                hi_list = []  # This will contain the dictionary list ready to insert to MEMSql

                for i in range(0, len(sorted_x)):
                    dicth = {}
                    dicth['ID'] = ID
                    dicth['level'] = i+1
                    dicth['value'] = sorted_x[i]['level']
                    hi_list.append(dicth)
                try:
                    logger.info('Inserting to cache..')
                    CC.insert_data(hi_list,'Hierarchy_table')
                    logger.info('Inserting to cache successful.')
                except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                if previous_lvl == 'All':
                    return json.dumps(hi_list)
                else:
                    return json.dumps(sorted_x[0]['level'])

            elif db == 'MSSQL':
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
                    result = json.loads(mssql.execute_query(query))
                    # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"},
                    # {"count": 8, "level": "vehicle_class"}]
                    logger.info("Data received!")
                    logger.debug("result %s" %result)
                except Exception, err:
                    logger.error('Error occurred while getting data from BigQuery Handler! %s' % err)
                    raise
                sorted_x = sorted(result, key=lambda k: k['count'])
                # Sort the dict to get the form the hierarchy (tuple is formed)
                hi_list = []  # This will contain the dictionary list ready to insert to MEMSql

                for i in range(0, len(sorted_x)):
                    dicth = {}
                    dicth['ID'] = ID
                    dicth['level'] = i+1
                    dicth['value'] = sorted_x[i]['level']
                    hi_list.append(dicth)
                try:
                    logger.info('Inserting to cache..')
                    CC.insert_data(hi_list,'Hierarchy_table')
                    logger.info('Inserting to cache successful.')
                except Exception, err:
                    logger.error("Cache insertion failed. %s" % err)
                if previous_lvl == 'All':
                    return json.dumps(hi_list)
                else:
                    return json.dumps(sorted_x[0]['level'])

        else:
            conditions = 'level = %s' % str(int(previous_lvl)+1)
            logger.info("Getting data from cache..")
            mem_data = CC.get_data('Hierarchy_table','value', conditions)  # dictionary
            logger.info("Data received! %s" % mem_data )
            try:
                level = mem_data['rows'][0][0]
                logger.info("Returned: %s" % level )
                return json.dumps(level)
            except:
                logger.warning("Nothing to return, End of hierarchy!")
                return 'End of hierarchy'



# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by=%{27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=sum&tablename=[digin_hnb.hnb_claims]&agg_f=[%27a3%27,%27b3%27,%27c3%27]
# http://localhost:8080/aggregatefields?group_by={%27vehicle_type%27:1}&order_by={}&agg=sum&tablename=[digin_hnb.hnb_claims1]&agg_f=[%27claim_cost%27]
# localhost:8080/aggregatefields?group_by={'a1':1,'b1':2,'c1':3}&order_by={'a2':1,'b2':2,'c2':3}&agg={'field1' : 'sum', 'field2' : 'avg'}&tablenames={1 : 'table1', 2:'table2', 3: 'table3'}&cons=a1=2&joins={1 : 'left outer join', 2 : 'inner join'}&join_keys={1: 'ON table1.field1' , 2: 'ON table2.field2'}&db=MSSQL
# for Single table:
# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by={%27a2%27:1,%27b2%27:2,%27c2%27:3}&agg={%27field1%27%20:%20%27sum%27,%20%27field2%27%20:%20%27avg%27}&tablenames={1%20:%20%27table1%27,%202:%27table2%27,%203:%20%27table3%27}&cons=a1=2&db=MSSQL
class AggregateFields():
    def GET(self, r):

        group_bys_dict = ast.literal_eval(web.input().group_by)  # {'a1':1,'b1':2,'c1':3}
        order_bys_dict = ast.literal_eval(web.input().order_by)  # {'a2':1,'b2':2,'c2':3}
        tablenames = ast.literal_eval(web.input().tablenames)  # 'tablenames' {1 : 'table1', 2:'table2', 3: 'table3'}
        aggregations = ast.literal_eval(web.input().agg) # {'field1' : 'sum', 'field2 : 'avg'}
        conditions = web.input().cons # ''
        try:
            join_types = ast.literal_eval(web.input().joins) # {1 : 'left outer join', 2 : 'inner join'}
            join_keys = ast.literal_eval(web.input().join_keys) # {1: 'ON table1.field1' , 2: 'ON table2.field2'}
        except AttributeError:
            logger.info("Single table query received")
            join_types = {}
            join_keys = {}
            pass
        db = web.input().db

        # SELECT a2, b2, c2, a1, b1, c1, sum(a3), sum(b3), sum(c3) FROM tablenames GROUP BY a1, b1, c1 ORDER BY a2, b2, c2

        if db == 'MSSQL':
            logger.info("MSSQL - Processing started!")
            if aggregations.itervalues().next() != 'percentage':
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
                for key, value in aggregations.iteritems():
                    altered_field = key.replace('.','_')
                    aggregation_fields = '{0}({1}) as {2}_{3}'.format(value, key, value, altered_field)
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
                    result = mssql.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from sql Handler!')
                    logger.error(err)

                result_dict = json.loads(result)
                logger.info("MSSQL - Processing completed!")
                return json.dumps(result_dict)

            elif aggregations.itervalues().next() == 'percentage':
                result = cfg.percentage('MSSQL', tablenames[1],aggregations.iterkeys().next(),None)
                return result

        elif db == 'BigQuery':

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
            for key, value in aggregations.iteritems():
                altered_field = key.replace('.','_')
                aggregation_fields = '{0}({1}) as {2}_{3}'.format(value, key, value, altered_field)
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
                result = BQ.execute_query(query)
                logger.info('Data received!')
                logger.debug('Result %s' % result)
            except Exception, err:
                logger.error('Error occurred while getting data from BQ Handler!')
                logger.error(err)
            result_dict = json.loads(result)
            logger.info("BigQuery - Processing completed!")
            return json.dumps(result_dict)



#http://localhost:8080/hierarchicalsummary?h={%22vehicle_usage%22:1,%22vehicle_type%22:2,%22vehicle_class%22:3}&tablename=[digin_hnb.hnb_claims]&conditions=date%20=%20%272015-05-04%27%20and%20name=%27marlon%27&id=1
class createHierarchicalSummary(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        table_name = web.input().tablename
        dictb = ast.literal_eval(web.input().h)
        db = web.input().db
        ID = int(web.input().id)
        # dictb = {"vehicle_usage":1,"vehicle_type":2,"vehicle_class":3}
        tup = sorted(dictb.items(), key=operator.itemgetter(1))
        where_clause = ''
        try:
            conditions = web.input().conditions
            where_clause = 'WHERE %s' % conditions
        except:
            pass
        logger.info('Requested received: %s' % web.input().values())
        fields = []  # ['aaaa', 'bbbb', 'cccc']
        counted_fields = []
        partition_by = []
        count_statement = []
        window_functions_set = []

        cache_state = None
        try:
            cache_state = CC.get_data('Hierarchy_summary','is_expired','ID=%s'%ID).is_expired
        except:
            logger.info("No data in cache")

        if cache_state == 1 or cache_state is None:
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
            if db == 'BigQuery':
                try:
                    result = BQ.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from BigQuery Handler! %s' % err)
                    raise
            elif db == 'MSSQL':
                try:
                    result = mssql.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from sql Handler! %s' % err)
                    raise

            result_dict = json.loads(result)
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

            final_result_json = json.dumps(final_result)
            logger.info('Data processed successfully...')
            try:
                CC.insert_data([{'ID': ID, 'createddatetime': str(datetime.datetime.now()),
                                 'data': final_result_json, 'is_expired': 0}], 'Hierarchy_summary')
                logger.info("Cache Update Successful")
            except Exception, err:
                logger.error("Error in updating cache. %s" % err)
                pass

            return final_result_json
        else:
            logger.info("Getting Hierarchy_summary data from Cache..")
            try:
                data = CC.get_data('Hierarchy_summary', 'data', 'ID=%s ' % ID).data
                logger.info("Data received from cache")
                return json.dumps(json.loads(data))
            except:
                logger.error("Error occurred while fetching data from Cache")
                return "Error"

class RefineGeoLocations(web.storage):
    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        table_name = web.input().table_name
        field_name1 = web.input().f_name1
        field_name2 = web.input().f_name2
        field_name3 = web.input().f_name3
        field_name4 = web.input().f_name4
        deliminator = web.input().delim

        data = splitter.sqlsplit(table_name, field_name1, field_name2, field_name3, field_name4, deliminator)

        return data


#http://localhost:8080/forecast?model=additive&pred_error_level=0.0001&alpha=0&beta=53&gamma=34&fcast_days=30&table_name=[Demo.forcast_superstoresales]&field_name_d=Date&field_name_f=Sales&steps_pday=1
class Forecasting_1():
    def GET(self, r):
        fcast_days = int(web.input().fcast_days)
        timesteps_per_day = int(web.input().steps_pday)
        pred_error_level = float(web.input().pred_error_level)
        model = str(web.input().model)
        m = int(web.input().m)
        alpha = web.input().alpha
        beta = web.input().beta
        gamma = web.input().gamma
        table_name = web.input().table_name
        field_name_date = web.input().field_name_d
        field_name_forecast = web.input().field_name_f
        interval = str(web.input().interval)
        null = None

        if interval == 'Daily':
            query = "SELECT TIMESTAMP_TO_SEC({0}) as date, SUM({1}) as value from {2} group by date order by date".format(field_name_date,field_name_forecast,table_name)
        # elif plot_interval == 'Monthly':
        #     query = "SELECT TIMESTAMP_TO_SEC(TIMESTAMP(concat(string(STRFTIME_UTC_USEC({0}, '%Y')),'-', string(STRFTIME_UTC_USEC({1}, '%m')),'-','01'))) as date, FLOAT(SUM({2})) as value FROM {3} GROUP BY  date ORDER BY  date".format(field_name_date, field_name_date, field_name_forecast, table_name)
            result = json.loads(BQ.execute_query(query))

            datapoints = []
            for row in result:
                datapoints.append([row['value'], row['date']])
            data_in = [{"target": "Historical_values", "datapoints": datapoints}]

            #translate the data.  There may be better ways if you're
            #prepared to use pandas / input data is proper json
            time_series = data_in[0]["datapoints"]

            epoch_in = []
            Y_observed = []

            for (y,x) in time_series:
                if y and x:
                    epoch_in.append(x)
                    Y_observed.append(y)

            #Pass in the number of days to forecast
            #fcast_days = 30
            res = FP.holt_predict(Y_observed,epoch_in,model,m,fcast_days,pred_error_level,timesteps_per_day)
            data_out = data_in + res

            for i in range( len( data_out ) ):
                if data_out[i]['target'] != 'RMSE' and data_out[i]['target'] != 'TotalForecastedVal':
                    for j in range(len(data_out[i]['datapoints'])):
                        lst = list(data_out[i]['datapoints'][j])
                        casted_datetime = datetime.datetime.fromtimestamp(lst[1]).strftime('%Y-%m-%d')
                        data_out[i]['datapoints'][j] = (lst[0],casted_datetime)

            # print json.dumps(data_out)
            # import matplotlib.pyplot as plt
            # plt.plot(epoch_in,Y_observed)
            # m,tstamps = zip(*res[0]['datapoints'])
            # u,tstamps = zip(*res[1]['datapoints'])
            # l,tstamps = zip(*res[2]['datapoints'])
            # plt.plot(tstamps,u, label='upper')
            # plt.plot(tstamps,l, label='lower')
            # plt.plot(tstamps,m, label='mean')
            # plt.show()
            return json.dumps(data_out)

        elif interval == 'Monthly':

            #query = "SELECT TIMESTAMP_TO_SEC({0}) as date, SUM({1}) as value from {2} group by date order by date".format(field_name_date,field_name_forecast,table_name)
            query = "SELECT TIMESTAMP_TO_SEC(TIMESTAMP(concat(string(STRFTIME_UTC_USEC({0}, '%Y')),'-', string(STRFTIME_UTC_USEC({1}, '%m')),'-','01'))) as date, FLOAT(SUM({2})) as value FROM {3} GROUP BY  date ORDER BY  date".format(field_name_date, field_name_date, field_name_forecast, table_name)

            #print query
            result = json.loads(BQ.execute_query(query))


            datapoints = []
            for row in result:
                datapoints.append([row['value'], row['date']])
            data_in = [{"target": "average", "datapoints": datapoints}]

            #translate the data.  There may be better ways if you're
            #prepared to use pandas / input data is proper json
            time_series = data_in[0]["datapoints"]

            epoch_in = []
            Y_observed = []

            for (y,x) in time_series:
                if y and x:
                    epoch_in.append(x)
                    Y_observed.append(y)

            #Pass in the number of days to forecast
            #fcast_days = 30
            res = FP.holt_predict(Y_observed,epoch_in,model,m,fcast_days,pred_error_level,timesteps_per_day,)
            data_out = data_in + res
            for i in range( len( data_out ) ):
                if data_out[i]['target'] != 'RMSE' and data_out[i]['target'] != 'TotalForecastedVal':
                    for j in range(len(data_out[i]['datapoints'])):
                        lst = list(data_out[i]['datapoints'][j])
                        casted_datetime = datetime.datetime.fromtimestamp(lst[1]).strftime('%Y-%m-%d')
                        data_out[i]['datapoints'][j] = (lst[0],casted_datetime)
            # print json.dumps(data_out)
            # import matplotlib.pyplot as plt
            # plt.plot(epoch_in,Y_observed)
            # m,tstamps = zip(*res[0]['datapoints'])
            # u,tstamps = zip(*res[1]['datapoints'])
            # l,tstamps = zip(*res[2]['datapoints'])
            # plt.plot(tstamps,u, label='upper')
            # plt.plot(tstamps,l, label='lower')
            # plt.plot(tstamps,m, label='mean')
            # plt.show()
            return json.dumps(data_out)



class GetHistoricalGroupedData():
    def GET(self, r):
        interval = web.input().interval
        table_name = web.input().table_name
        field_name_date = web.input().field_name_d
        field_name_unit = web.input().field_name_u
        field_name_amount = web.input().field_name_a

        if interval == 'MONTHLY':
            query = "SELECT STRFTIME_UTC_USEC({0}, '%Y') as year, STRFTIME_UTC_USEC({1}, '%m') as month, SUM({2}) as sales, SUM({3}) as tot_units " \
                    "FROM [{4}] GROUP BY year, month ORDER BY year, month"\
                    .format(field_name_date,field_name_date,field_name_amount,field_name_unit,table_name)
            data_set = BQ.execute_query(query)
            return data_set

if __name__ == "__main__":
    app.run()
