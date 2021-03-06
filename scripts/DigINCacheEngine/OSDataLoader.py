__author__ = 'Marlon'

import sys
import os
sys.path.append('/modules')
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
import modules.ObjectStoreDataHandler as OS
import web
import CacheController as CC
import json

urls =(
    '/loadtocache/(.*)', 'LoadToCache',
    '/prepareenv', 'PrepareEnv'
)

app = web.application(urls, globals())

class LoadToCache:
    #http://localhost:8080/loadtocache/com.duosoftware.com/DemoHNB_claim?skip=0&take=2
    def GET(self,indexname):
        data = OS.get_data('gg', 'gt', indexname)
        print data
        result = CC.insert_data(json.loads(data),indexname)
        #CC.create_table(indexname,fieldswithtypes)
        #TODO
        return result

class PrepareEnv:
    #http://localhost:8080/prepareenv
    #TODO NOT FINISHED
    def POST(self):
        post_data = web.input()
        fields_types = post_data.fields_types
        table_name = post_data.table_name
        print fields_types
        print table_name
        #post_data=web.input(name=[])
        CC.create_table(fields_types,table_name)

if  __name__ == "__main__":
    app.run()