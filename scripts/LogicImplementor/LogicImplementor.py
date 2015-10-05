__author__ = 'Marlon'

import sys
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/modules')
import ObjectStoreDataHandler as OS
import web
import json
import collections

print 'inside Logic imp'

urls = (
    '/hierarchicalsummary(.*)', 'createHierarchicalSummary'
)

app = web.application(urls, globals())

#OS.callOS('dd','lg','com.duosoftware.com', 'DemoHNB_claim','')
def convert(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data

def merge(lsta, lstb):
        print 'inside merge'
        for i in lstb:
            for j in lsta:
                if j['VEHICLE_CLASS'] == i['VEHICLE_CLASS']:
                    j.update(i)
                    break
            else:
                lsta.append(i)

            return
#http://localhost:8080/hierarchicalsummary?indexname=com.duosoftware.com&type1=DemoHNB_claim
class createHierarchicalSummary(web.storage):
    def GET (self,r):

        indexname = web.input().indexname
        type1 = web.input().type1
        i = 0
        dicta = {}
        dictb = {}
        while 1:
            path = indexname +'/' + type1 +  '?' + 'skip=' + str(i) +'&take='+ str(i+500)
            print path
            result = OS.callOS('dd','lg',path)
            dictb = json.loads(result)
            #print 'values: %s' % dictb.items()
            print 'dicta: ' + str(dicta)
            print 'dictb: ' + str(dictb) + 'skip' + str(i) + 'take :' + str(i)
           # cdictb = convert(dictb)
           # print cdictb
            if str(result) == "{}":
                break
            else:
                for k,v in dictb.items():
                    print 'k: ' + str(k) + ' v: ' + str(v)
                    merge(dicta.setdefault(k, []), str(v))

        print dicta
        return dicta



if  __name__ == "__main__":
    app.run()
