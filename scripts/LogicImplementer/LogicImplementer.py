__author__ = 'Marlon'

import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import scripts.DigINCacheEngine.CacheController as CC
import web
import json
import operator
import ast
import logging
import datetime

urls = (
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields'
)

app = web.application(urls, globals())

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('LogicImplementer.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')

#http://localhost:8080/hierarchicalsummary?h={%22vehicle_usage%22:1,%22vehicle_type%22:2,%22vehicle_class%22:3}&tablename=[digin_hnb.hnb_claims]
class createHierarchicalSummary(web.storage):

    def GET (self,r):

        tablename = web.input().tablename
        i = 0
        dictb = ast.literal_eval(web.input().h)
        #dictb = {"vehicle_usage":1,"vehicle_type":2,"vehicle_class":3}
        tup = sorted(dictb.items(), key=operator.itemgetter(1))
        logger.info('Args received: tablename = {0}, h = {1}'.format(tablename,dictb))
        fields = [] # ['aaaa', 'bbbb', 'cccc']
        counted_fields = []
        partition_by = []
        count_statement = []
        window_functions_set = []

        cache_state = None
        try:
            cache_state = CC.get_data('Hierarchy_summary','is_expired','ID=1').is_expired
        except:
            logger.info("No data in cache")

        if cache_state == 1 or cache_state is None:
            for i in range(0,len(tup)):
              fields.append(tup[i][0])
              counted_fields.append('%s_count' %(tup[i][0])) #['aaaa_count', 'bbbb_count', 'cccc_count']
              p = []
              count_statement.append('COUNT (%s) as %s_count' %(tup[i][0], tup[i][0]))
              for j in range (0,i+1):
                p.append(fields[j])
              p_str =  ', '.join(p)
              partition_by.append(p_str)
              window_functions = 'SUM(%s) OVER (PARTITION BY %s) as %s_count1' %(counted_fields[i], str(partition_by[i]), tup[i][0]) # SUM(cccc_count) OVER (PARTITION BY ['aaaa', 'bbbb', 'cccc']) as cccc_count1
              window_functions_set.append(window_functions)


            fields_str = ', '.join(fields)
            window_functions_set_str = ', '.join(window_functions_set)
            count_statement_str = ', '.join(count_statement)

            query = 'SELECT {0}, {1} FROM (SELECT {2}, {3} FROM {4} GROUP BY {5} )z ORDER BY {6}'.format(fields_str,window_functions_set_str,fields_str,count_statement_str,tablename, fields_str, fields_str)
            logger.info('Query formed successfully! : %s' %query)
            logger.info('Fetching data from BigQuery...')
            result = ''
            try:
                result = BQ.execute_query(query)
                logger.info('Data received!')
                logger.debug('Result %s' %result)
            except:
                logger.error('Error occurred while getting data from BigQuery Handler!')
            result_dict = json.loads(result)
            # sets up json
            #levels_memory = {'vehicle_usage': [], 'vehicle_type': [], 'vehicle_class': []}
            levels_memory_f = []
            levels_memrory_str = '{%s}'
            for i in range(0,len(fields)):
                 levels_memory_f.append("'{0}': []".format(fields[i]))
            levels_index = dict (zip(dictb.values(),dictb.keys()))
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
                levels_memory = ast.literal_eval(levels_memrory_str % ' ,'.join(levels_memory_f))
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
            final_result = build_level(result_dict,1)
            final_result_json = json.dumps(final_result)
            logger.info('Data processed successfully...')
            try:
                CC.insert_data([{'ID' : 1, 'createddatetime' : str(datetime.datetime.now()), 'data' : final_result_json, 'is_expired' : 0}],'Hierarchy_summary')
                logger.info("Cache Update Successful")
            except:
                logger.error("Error in updating cache")

            return final_result_json
        else:
            logger.info("Getting Hierarchy_summary data from Cache..")
            try:
                data = CC.get_data('Hierarchy_summary','data','ID=1').data
                logger.info("Data received from cache")
                return json.dumps(json.loads(data))
            except:
                logger.error("Error occurred while fetching data from Cache")
                return "Error"

#http://localhost:8080/gethighestlevel?tablename=[digin_hnb.hnb_claims]&type=DemoHNB_claim&levels=['vehicle_usage','vehicle_class','vehicle_type']&plvl=All
class getHighestLevel(web.storage):
    def GET(self,r):
        logging.info("Entered getHighestLevel.")
        tablename = web.input().tablename
        type = web.input().type
        levels =  [ item.encode('ascii') for item in ast.literal_eval(web.input().levels) ]
        try:
            previous_lvl = web.input().plvl
        except:
            previous_lvl = ''   # If plvl is not sent assign an empty string

        logger.info("Received args: tablename: {0}, type: {1}, levels: {2}, plvl: {3}".format(tablename,type,levels,previous_lvl) )

        #check_result = CC.get_data(('Hierarchy_table','value',conditions))
        if len(previous_lvl) == 0 or previous_lvl == 'All': # If plvl is not sent going to create the hierarchy assuming the data is not there in MEMSQL

            query = 'select count(level) as count, level from  {0}  group by level'
            sub_body = []
            for i in range(0,len(levels)):
               sub_body.append('(select {0}, "{1}" as level from {2} group by {3})'.format(levels[i],levels[i],tablename,levels[i]))
            sub_body_str = ' ,'.join(sub_body)
            query = query.format(sub_body_str)  # UNION is not supported in BigQuery
            logger.info("Query formed! %s" %query)
            logger.info("Fetching data from BigQuery..")
            result = ''
            try:
                 result = json.loads(BQ.execute_query(query)) # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"}, {"count": 8, "level": "vehicle_class"}]
                 logger.info("Data received!")
                 logger.info("result %s" %result)
            except:
                 logger.info(result)
                 logger.error("Error occurred while getting data from BigQuery")
            sorted_x = sorted(result, key= lambda k :k['count']) # Sort the dict to get the form the hierarchy (tuple is formed)
            hi_list = [] # This will contain the dictionary list ready to insert to MEMSql

            for i in range(0, len(sorted_x)):
                dicth = {}
                dicth['ID'] = type
                dicth['level'] = i+1
                dicth['value'] = sorted_x[i]['level']
                hi_list.append(dicth)
            try:
                logger.info('Inserting to cache..')
                CC.insert_data(hi_list,'Hierarchy_table')
                logger.info('Inserting to cache successful.')
            except:
                logger.info("Cache insertion failed")
            if previous_lvl == 'All':
                return hi_list
            else:
                return sorted_x[0]['level']

        else:
            conditions = 'level = %s' % str(int(previous_lvl)+1)
            logger.info("Getting data from cache..")
            mem_data = CC.get_data('Hierarchy_table','value',conditions)  # dictionary
            logger.info("Data received!")
            try:
                level = mem_data['rows'][0][0]
                logger.info("Returned: %s" %level)
                return  level
            except:
                logger.warning("Nothing to return, End of hierarchy!")
                return  'End of hierarchy'


#http://localhost:8080/aggregatefields?tablename=[digin_hnb.hnb_claims]&field=claim_cost&agg=avg&ID=1
class AggregateFields():
    def GET(self,r):
        logger.info("Entered AggregateFields!")
        tablename = web.input().tablename
        field_to_aggregate = web.input().field
        type_of_aggregation = web.input().agg
        ID = int(web.input().ID)
        logger.info("Args received: tablename: {0}, field: {1}, agg: {2}".format(tablename,field_to_aggregate,type_of_aggregation))
        if type_of_aggregation == 'sum':
            cache_state = None
            try:
                cache_state = CC.get_data('digin_aggregations','is_expired','ID=%s and type="sum"' %ID).is_expired
            except:
                logger.info("No data in cache")
                logger.info("Getting data from Cache..")
            if cache_state == 1 or cache_state is None:
                logger.info("Getting data from BQ")
                sum_result = BQ.execute_query('SELECT SUM(FLOAT({0})) FROM {1}'.format(field_to_aggregate,tablename))
                logger.info("Data received and start inserting to cache")
                CC.insert_data([{"ID":ID, "type":"sum","value":sum_result,"is_expired":0}],'digin_aggregations')
                logger.info("Cache updated")
                logger.info("Summation: %s" %sum_result)
                return sum_result
            else:
                sum_result = CC.get_data('digin_aggregations','value','ID=%s and type="sum"' %ID).value
                logger.info("Data received")
                return sum_result

        elif type_of_aggregation == 'count':
            cache_state = None
            try:
                cache_state = CC.get_data('digin_aggregations','is_expired','ID=%s and type="count"' %ID).is_expired
            except:
                logger.info("No data in cache")
                logger.info("Getting data from Cache..")
            if cache_state == 1 or cache_state is None:
                logger.info("Getting data from BQ")
                count_result = BQ.execute_query('SELECT COUNT(*) FROM {0};'.format(tablename))
                logger.info("Data received and start inserting to cache")
                CC.insert_data([{"ID":ID, "type":"count","value":count_result,"is_expired":0}],'digin_aggregations')
                logger.info("Cache updated")
                logger.info("Counted: %s" %count_result)
                return count_result
            else:
                count_result = CC.get_data('digin_aggregations','value','ID=%s and type="count"' %ID).value
                logger.info("Data received")
                logger.info("Counted: %s" %count_result)
                return count_result



        elif type_of_aggregation == 'avg':
            cache_state = None
            try:
                cache_state = CC.get_data('digin_aggregations','is_expired','ID=%s and type="avg"'%ID).is_expired
            except:
                logger.info("No data in cache")
                logger.info("Getting data from Cache..")
            if cache_state == 1 or cache_state is None:
                logger.info("Getting data from BQ")
                avg_result = BQ.execute_query('SELECT AVG(FLOAT({0})) FROM {1}'.format(field_to_aggregate,tablename))
                logger.info("Data received and start inserting to cache")
                CC.insert_data([{"ID":ID, "type":"avg","value":avg_result,"is_expired":0}],'digin_aggregations')
                logger.info("Cache updated")
                logger.info("avg: %s" %avg_result)
                return avg_result
            else:
                avg_result = CC.get_data('digin_aggregations','value','ID=%s and type="avg"' %ID).value
                logger.info("Data received")
                logger.info("avg: %s" %avg_result)
                return avg_result


        else:
            logger.error("Incorrect aggregation requested!")
            return 'Incorrect aggregation requested!'



if  __name__ == "__main__":
    app.run()
