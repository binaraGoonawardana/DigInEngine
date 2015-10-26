__author__ = 'Marlon'

import sys
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/modules')
import BigQueryHandler as BQ
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/scripts/DigINCacheEngine')
import CacheController as CC
import web
import json
import operator
import ast

urls = (
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields'
)

app = web.application(urls, globals())


#http://localhost:8080/hierarchicalsummary?h={%22vehicle_usage%22:1,%22vehicle_type%22:2,%22vehicle_class%22:3}&tablename=[digin_hnb.hnb_claims]
class createHierarchicalSummary(web.storage):

    def GET (self,r):

        tablename = web.input().tablename
        i = 0
        dictb = ast.literal_eval(web.input().h)
        #dictb = {"vehicle_usage":1,"vehicle_type":2,"vehicle_class":3}
        tup = sorted(dictb.items(), key=operator.itemgetter(1))

        fields = [] # ['aaaa', 'bbbb', 'cccc']
        counted_fields = []
        partition_by = []
        count_statement = []
        window_functions_set = []

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
        print query

        result = BQ.execute_query(query)
        print result
        result_dict = json.loads(result)
        # sets up json
        #levels_memory = {'vehicle_usage': [], 'vehicle_type': [], 'vehicle_class': []}
        levels_index = dict (zip(dictb.values(),dictb.keys()))
        result = []

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
            levels_memory = {'vehicle_usage': [], 'vehicle_type': [], 'vehicle_class': []}
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
        #CC.insert_data([{'ID' : 1, 'createddatetime' : str(datetime.datetime.now()), 'data' : final_result}],'Hierarchy_summary')

        return final_result_json

#http://localhost:8080/gethighestlevel?tablename=[digin_hnb.hnb_claims]&type1=DemoHNB_claim&levels=['vehicle_usage','vehicle_class','vehicle_type']&plvl=All
class getHighestLevel(web.storage):
    def GET(self,r):
        tablename = web.input().tablename
        type1 = web.input().type1
        levels =  [ item.encode('ascii') for item in ast.literal_eval(web.input().levels) ]
        try:
            previous_lvl = web.input().plvl
        except:
            previous_lvl = ''   # If plvl is not sent assign an empty string

        #check_result = CC.get_data(('Hierarchy_table','value',conditions))
        if len(previous_lvl) == 0 or previous_lvl == 'All': # If plvl is not sent going to create the hierarchy assuming the data is not there in MEMSQL

            query = 'select count(level) as count, level from  {0}  group by level'
            sub_body = []
            for i in range(0,len(levels)):
               sub_body.append('(select {0}, "{1}" as level from {2} group by {3})'.format(levels[i],levels[i],tablename,levels[i]))
            sub_body_str = ' ,'.join(sub_body)
            query = query.format(sub_body_str)  # UNION is not supported in BigQuery
            print query

            result = json.loads(BQ.execute_query(query)) # get data from BQ [{"count": 5, "level": "vehicle_usage"}, {"count": 23, "level": "vehicle_type"}, {"count": 8, "level": "vehicle_class"}]
            print result
            sorted_x = sorted(result, key= lambda k :k['count']) # Sort the dict to get the form the hierarchy (tuple is formed)
            print 'sorted x'
            hi_list = [] # This will contain the dictionary list ready to insert to MEMSql

            for i in range(0, len(sorted_x)):
                dicth = {}
                dicth['ID'] = type1
                dicth['level'] = i+1
                dicth['value'] = sorted_x[i]['level']
                hi_list.append(dicth)
            try:
                CC.insert_data(hi_list,'Hierarchy_table')
            except:
                "Insertion failed"
            if previous_lvl == 'All':
                return hi_list
            else:
                return sorted_x[0]['level']

        else:
            conditions = 'level = %s' % str(int(previous_lvl)+1)
            mem_data = CC.get_data('Hierarchy_table','value',conditions)  # dictionary
            try:
                print mem_data['rows'][0][0]
                level = mem_data['rows'][0][0]
                return  level
            except:
                return  'End of hierarchy'


#http://localhost:8080/aggregatefields?tablename=DemoHNB_claim&field=claim_cost&agg=avg
class AggregateFields():
    def GET(self,r):
        tablename = web.input().tablename
        field_to_aggregate = web.input().field
        type_of_aggregation = web.input().agg
        #path = indexname +'/' + type +  '?skip=0&take=50'
        #result = json.loads(OS.get_data('dd','lg',path))

        if type_of_aggregation == 'sum':
            #sum1 = sum([item[field_to_aggregate] for item in result])
            #return sum1
            sum_result = CC.get_data(tablename, 'sum(%s)' %field_to_aggregate, '')
            return sum_result
        elif type_of_aggregation == 'count':
            count_result = CC.get_data(tablename, 'count(%s)' %field_to_aggregate, '')
            return count_result
        elif type_of_aggregation == 'avg':
            avg_result = CC.get_data(tablename,'avg(%s)' %field_to_aggregate, '' )
            return avg_result
        else:
            return 'Incorrect aggregation requested!'



if  __name__ == "__main__":
    app.run()
