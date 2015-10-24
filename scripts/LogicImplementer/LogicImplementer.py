__author__ = 'Marlon'

import sys
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/modules')
import BigQueryHandler as BQ
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/scripts/DigINCacheEngine')
import CacheController as CC
import web
import json
from operator import itemgetter
from itertools import groupby
import collections
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

        return json.dumps(final_result)

#http://localhost:8080/gethighestlevel?tablename=[digin_hnb.hnb_claims]&type1=DemoHNB_claim&lvl1=vehicle_usage&lvl2=vehicle_class&lvl3=vehicle_type
class getHighestLevel(web.storage):
    def GET(self,r):
        tablename = web.input().tablename
        type1 = web.input().type1

        groupby1 = web.input().lvl1
        groupby2 = web.input().lvl2
        groupby3 = web.input().lvl3
        try:
            previous_lvl = web.input().plvl
        except:
            previous_lvl = ''   # If plvl is not sent assign an empty string
        h1 = ''
        dict_hierarchy ={}

        #check_result = CC.get_data(('Hierarchy_table','value',conditions))
        if len(previous_lvl) == 0 or previous_lvl == 'All': # If plvl is not sent going to create the hierarchy assuming the data is not there in MEMSQL
            result = json.loads(BQ.execute_query('SELECT * FROM %s' % tablename)) # get data from OS
            unique_counts_groupby1 = collections.Counter(e[groupby1] for e in result) # Count unique members field specified by groupby1
            unique_counts_groupby1_len = len(unique_counts_groupby1)
            dict_hierarchy[groupby1] = unique_counts_groupby1_len

            unique_counts_groupby2 = collections.Counter(e[groupby2] for e in result)
            unique_counts_groupby2_len = len(unique_counts_groupby2)
            dict_hierarchy[groupby2] = unique_counts_groupby2_len

            unique_counts_groupby3 = collections.Counter(e[groupby3] for e in result)
            unique_counts_groupby3_len = len(unique_counts_groupby3)
            dict_hierarchy[groupby3] = unique_counts_groupby3_len

            sorted_x = sorted(dict_hierarchy.items(), key=operator.itemgetter(1)) # Sort the dict to get the form the hierarchy (tuple is formed)
            print 'sorted x'
            print sorted_x
            hi_list = [] # This will contain the dictionary list ready to insert to MEMSql

            for i in range(0, len(sorted_x)):
                dicth = {}
                dicth['ID'] = type1
                dicth['level'] = i+1
                dicth['value'] = sorted_x[i][0]
                print i
                print dicth
                hi_list.append(dicth)
                print hi_list
            try:
                CC.insert_data(hi_list,'Hierarchy_table')
            except:
                "Insertion failed"
            if previous_lvl == 'All':
                return hi_list
            else:
                return sorted_x[0][0]

        else:
            conditions = 'level = %s' % str(int(previous_lvl)+1)
            mem_data = CC.get_data('Hierarchy_table','value',conditions)  # dictionary
            try:
                print mem_data['rows'][0][0]
                level = mem_data['rows'][0][0]
                return  level
            except:
                return  'End of hierarchy'


#http://localhost:8080/aggrgatefields?tablename=DemoHNB_claim&field=claim_cost&agg=avg
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



if  __name__ == "__main__":
    app.run()
