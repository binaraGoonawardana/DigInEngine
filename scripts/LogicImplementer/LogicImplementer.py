__author__ = 'Marlon'

import sys
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/modules')
import ObjectStoreDataHandler as OS
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/scripts/DigINCacheEngine')
import CacheController as CC
import web
import json
from operator import itemgetter
from itertools import groupby
import collections
import operator

print 'inside Logic imp'

urls = (
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary',
    '/gethighestlevel(.*)', 'getHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields'
)

app = web.application(urls, globals())

#OS.callOS('dd','lg','com.duosoftware.com', 'DemoHNB_claim','')



#http://localhost:8080/hierarchicalsummary?indexname=com.duosoftware.com&type1=DemoHNB_claim
class createHierarchicalSummary(web.storage):
    def GET (self,r):

        indexname = web.input().indexname
        type1 = web.input().type1
        i = 0
        dictb = {}
        path = indexname +'/' + type1 #+  '?skip=0&take=500'
        print path
        result = OS.get_data('dd','lg',path)
        dictb = json.loads(result)
        print dictb
        dictb.sort(key=itemgetter('VEHICLE_CLASS','VEHICLE_TYPE','VEHICLE_USAGE'))

        # Iterate in groups
        dictq = {}
        dictq['size'] = 0
        dictq['imageURL'] = 0
        dictq['parent'] = []
        h1counter = 0
        h2counter = 0
        h3counter = 0
        for h1, items in groupby(dictb, key=itemgetter('VEHICLE_CLASS','VEHICLE_TYPE','VEHICLE_USAGE')): #gives one claim
             print 'first loop'
             print(h1)

             h1counter += 1


             hierarchy1 = h1[0]
             hierarchy2 = h1[1]
             hierarchy3 = h1[2]

             dictq['parent'].append({'name':hierarchy1,'imageURL':0,'type':0,'size':0,'children':[
                                {'name':hierarchy2,'imageURL':0,'type':0,'size':0,'children':[
                                {'name':hierarchy3,'imageURL':0,'type':0,'size':0}]}
                                ]})
             print json.dumps(dictq)
             #dictq['parent'] = [{'name':hierarchy1,'imageURL':0,'type':0,'size':0,'children':[
             #                    {'name':hierarchy2,'imageURL':0,'type':0,'size':0,'children':[
             #                   {'name':hierarchy3,'imageURL':0,'type':0,'size':0}]}
             #                  ]}]


             for i in items:        #gives grouped claims execute per group
                print '2nd loop'
                print('    ', i)
                h2counter += 1
                for key in i:
                    h3counter += 1
                    print '3rd loop'
                    count = i['CLAIM_COST']
                    print 'h1counter: ', h1counter
                dictq['parent'][h1counter-1]['children'][0]['size'] = h3counter
                print 'dict'

                #dictq['hierarchy3'] = hierarchy3
                #dictq['hierarchy2'] = hierarchy2

                #dictq['count'] = count
             dictq['parent'][h1counter-1]['size'] = h2counter
        dictq['size'] = h1counter




        # dictq["children"] = [{'name':0,'imageURL':0,'type':0,'size':0,
         #                      'children' :[{'name':0,'imageURL':0,'type':0,'size':0} for k in range(5)]
          #                     } for k in range(5)]


#dictionary = {}
#dictionary["new key"] = "some new entry" # add new dictionary entry
#dictionary["dictionary_within_a_dictionary"] = {} # this is required by python
#dictionary["dictionary_within_a_dictionary"]["sub_dict"] = {"other" : "dictionary"}
#http://stackoverflow.com/questions/1024847/add-key-to-a-dictionary-in-python
#print (dictionary)
            #dictq['sum'] = sum(i['CLAIM_COST'])

        print (dictq)
        return json.dumps(dictq)

#http://localhost:8080/gethighestlevel?indexname=com.duosoftware.com&type1=DemoHNB_claim&lvl1=VEHICLE_USAGE&lvl2=VEHICLE_CLASS&lvl3=VEHICLE_TYPE
class getHighestLevel(web.storage):
    def GET(self,r):
        indexname = web.input().indexname
        type1 = web.input().type1
        path = indexname +'/' + type1 +  '?skip=0&take=50'
        print path

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
            result = json.loads(OS.get_data('dd','lg',path)) # get data from OS
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


#http://localhost:8080/aggregatefields?indexname=com.duosoftware.com&type=DemoHNB_claim&field=CLAIM_COST&agg=sum
class AggregateFields():
    def GET(self,r):
        indexname = web.input().indexname
        type = web.input().type
        field_to_aggregate = web.input().field
        type_of_aggregation = web.input().agg
        #path = indexname +'/' + type +  '?skip=0&take=50'
        #result = json.loads(OS.get_data('dd','lg',path))

        if type_of_aggregation == 'sum':
            #sum1 = sum([item[field_to_aggregate] for item in result])
            #return sum1
            sum_result = CC.get_data(type, 'sum(%s)' %field_to_aggregate, '')
            return sum_result
        elif type_of_aggregation == 'count':
            count_result = CC.get_data(type, 'count(%s)'%field_to_aggregate, '')
            return count_result


    #If agreed to get list use sorted(unique_counts_groupby1_len,unique_counts_groupby2_len,unique_counts_groupby3_len)
if  __name__ == "__main__":
    app.run()
