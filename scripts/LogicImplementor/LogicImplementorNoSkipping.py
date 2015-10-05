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

        path = indexname +'/' + type1 +  '?skip=0&take=500'
        print path
        result = OS.callOS('dd','lg',path)


        dictb = json.loads(result)


        print dictb


        #dictb.insert(0,0)
        #print 'values: %s' % dictb.items()

        dictb.sort(key=itemgetter('VEHICLE_CLASS','VEHICLE_TYPE','VEHICLE_USAGE'))

        # Iterate in groups
        #dictb['total'] = 0
        size = 0
        groupsize = 0

        groupsizecounter = 0
        newdict = {}

        for h1, items in groupby(dictb, key=itemgetter('VEHICLE_CLASS','VEHICLE_TYPE','VEHICLE_USAGE')):
         print(h1)

         sizecounter = 0
         groupsizecounter += 1
         print 'groupsizecounter: ' + str(groupsizecounter)
         print dictb[groupsize]
         for i in items:
            sizecounter += 1
            print 'sizecounter: ' + str(sizecounter)
            #dictb[size] = str(sizecounter)
            print('    ', i)
            #dictb[sizecounter-0]['size'] = sizecounter
         newdict['size'] = sizecounter
         newdict['hierarchy3'] = dictb[0]
         print 'newdict: ' , newdict
         dictb[groupsizecounter-1]['grouptotal'] = groupsizecounter
        print (dictb)
        return json.dumps(dictb)



      #  print 'dictb: ' + str(dictb)




if  __name__ == "__main__":
    app.run()
