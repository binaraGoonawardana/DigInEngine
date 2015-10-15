__author__ = 'Marlon'

import sys
sys.path.insert(0,'/Users/Administrator/PycharmProjects/digin-engine/modules')
import ObjectStoreDataHandler as OS
import web
import CacheController as CC
import json

urls =(
    '/loadtocache/(.*)', 'LoadToCache'
)

app = web.application(urls, globals())

class LoadToCache:
    #http://localhost:8080/loadtocache/com.duosoftware.com/DemoHNB_claim?skip=0&take=2
    def GET(self,indexname):
        data = OS.get_data('gg', 'gt', indexname)
        print data
        result = CC.insert_data(json.loads(data,indexname))
        #CC.create_table(indexname,fieldswithtypes)
        #TODO
        return result




if  __name__ == "__main__":
    app.run()