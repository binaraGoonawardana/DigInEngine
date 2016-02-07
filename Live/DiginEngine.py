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
import sqlalchemy as sql
from pandas import DataFrame
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
from bigquery import get_client
import  BigQueryHandler as BQ
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
engine = sql.create_engine("mssql+pyodbc://apxAdmin:apx@localhost:1433/APX_APARMENTS?driver=SQL+Server+Native+Client+11.0")

metadata = sql.MetaData()
connection = engine.connect()
urls = (
    '/executeQuery(.*)', 'execute_query',
    '/GetFields(.*)', 'get_Fields',
    '/GetTables(.*)', 'get_Tables',
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields',
    '/refinegeoloc(.*)','RefineGeoLocations'
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
                  fields.append(row[3])
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



#http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by={%27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=sum&tablename=[digin_hnb.hnb_claims]&agg_f=[%27a3%27,%27b3%27,%27c3%27]
#http://localhost:8080/aggregatefields?group_by={%27vehicle_type%27:1}&order_by={}&agg=sum&tablename=[digin_hnb.hnb_claims1]&agg_f=[%27claim_cost%27]
class AggregateFields():
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        group_bys_dict = ast.literal_eval(web.input().group_by) #{'a1':1,'b1':2,'c1':3}
        order_bys_dict = {}
        try:
            order_bys_dict = ast.literal_eval(web.input().order_by) #{'a2':1,'b2':2,'c2':3}
        except:
            logger.info("No order by clause")
        aggregation_type = web.input().agg #'sum'
        tablename = web.input().tablename #'tablename'
        aggregation_fields = ast.literal_eval(web.input().agg_f) #['a3','b3','c3']
        try:
            conditions = web.input().cons
        except AttributeError:
            logger.info("No Where clause found")
            conditions = ''
            pass
        db = web.input().db

        #SELECT a2, b2, c2, a1, b1, c1, sum(a3), sum(b3), sum(c3) FROM tablename GROUP BY a1, b1, c1 ORDER BY a2, b2, c2

        grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))


        group_bys_str = ''
        group_bys_str_ = ''
        if 1 in group_bys_dict.values():
            group_bys = []
            for i in range(0,len(grp_tup)):
                group_bys.append(grp_tup[i][0])
            print group_bys
            group_bys_str_ = ', '.join(group_bys)
            group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
            print group_bys_str

        if order_bys_dict != {}:
            ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
            order_bys_str = ''
            order_bys_str_ = ''
            if 1 in order_bys_dict.values():
                Order_bys = []
                for i in range(0,len(ordr_tup)):
                    Order_bys.append(ordr_tup[i][0])
                print Order_bys
                order_bys_str_ = ', '.join(Order_bys)
                order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                print order_bys_str

        else:
            order_bys_str = ''

        aggregation_fields_set = []
        for i in range(0,len(aggregation_fields)):
            aggregation_fields_ = '{0}({1})'.format(aggregation_type, aggregation_fields[i])
            aggregation_fields_set.append(aggregation_fields_)
        aggregation_fields_str = ', '.join(aggregation_fields_set)

        print aggregation_fields_str

        if 1 not in group_bys_dict.values():
            fields_list = [order_bys_str_,aggregation_fields_str]

        elif 1 not in order_bys_dict.values():
            fields_list = [group_bys_str_,aggregation_fields_str]

        else:
            fields_list = [order_bys_str_,group_bys_str_,aggregation_fields_str]

        fields_str = ' ,'.join(fields_list)
        print fields_str
        query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str,tablename,conditions,group_bys_str,order_bys_str)
        logger.info('Query formed successfully! : %s' %query)
        logger.info('Fetching data from BigQuery...')
        result = ''
        if db == 'BQ':
            try:
                result = BQ.execute_query(query)
                logger.info('Data received!')
                logger.debug('Result %s' %result)
            except Exception, err:
                logger.error('Error occurred while getting data from BigQuery Handler!')
        elif db == 'MSSQL':
            try:
                result = mssql.execute_query(query)
                logger.info('Data received!')
              
            except Exception, err:
                logger.error('Error occurred while getting data from sql Handler!', err)
                logger.error('Error occurred while getting data from sql Handler!')

        else:
            #TODO implement for other dbs
            logger.error('DB not supported')
            raise
        result_dict = json.loads(result)
        print result_dict
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
            if db == 'BQ':
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

if __name__ == "__main__":
    app.run()
