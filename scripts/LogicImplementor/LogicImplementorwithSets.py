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
        dictq = {}
        path = indexname +'/' + type1 +  '?skip=0&take=500'
        print path
        result = OS.callOS('dd','lg',path)
        dictb = json.loads(result)
        print dictb
        #dictb.insert(0,0)
        #print 'values: %s' % dictb.items()
        dictb.sort(key=itemgetter('VEHICLE_CLASS','VEHICLE_TYPE','VEHICLE_USAGE'))
        # Iterate in groups
        for h1, items in groupby(dictb, key=itemgetter('VEHICLE_CLASS','VEHICLE_TYPE','VEHICLE_USAGE')): #gives one claim
         print 'first loop'
         print(h1)
         for i in items:        #gives grouped claims execute per group
            print '2nd loop'
            print('    ', i)
            hierarchy3 =''
            hierarchy2  =''
            hierarchy1 = ''
            count = ''
            for key in i:
                print '3rd loop'
                print key
                hierarchy3 = i['VEHICLE_CLASS']
                hierarchy2 = i['VEHICLE_TYPE']
                hierarchy1 = i['VEHICLE_USAGE']
                count = i['CLAIM_COST']
            print 'dict'
            dictq['hierarchy3'] = hierarchy3
            dictq['hierarchy2'] = hierarchy2
            dictq['hierarchy1'] = hierarchy1
            dictq['count'] = count
            #dictq['sum'] = sum(i['CLAIM_COST'])
            print dictq
         print dictq
        print (dictb)
        return json.dumps(dictb)
      #  print 'dictb: ' + str(dictb)
if  __name__ == "__main__":
    app.run()
