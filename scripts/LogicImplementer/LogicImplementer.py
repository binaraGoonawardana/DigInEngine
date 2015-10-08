__author__ = 'Marlon'

import sys
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/modules')
import ObjectStoreDataHandler as OS
import web
import json
from operator import itemgetter
from itertools import groupby

import collections

print 'inside Logic imp'

urls = (
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary'
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
        result = OS.callOS('dd','lg',path)


        dictb = json.loads(result)


        print dictb


        #dictb.insert(0,0)
        #print 'values: %s' % dictb.items()

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



      #  print 'dictb: ' + str(dictb)




if  __name__ == "__main__":
    app.run()
