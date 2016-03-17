__author__ = 'Marlon'

import sys,os
sys.path.append("...")
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import descriptive_proceesor as dp
import web
import ast
import configs.ConfigHandler as conf

datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
default_cache_timeout = datasource_settings['default_timeout_interval']

urls = (
    '/generateboxplot(.*)', 'BoxPlotGeneration',
    '/generatehist(.*)', 'HistogramGeneration',
    '/generatebubble(.*)', 'BubbleChart'
)

app = web.application(urls, globals())

#http://localhost:8080/generateboxplot?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery&ID=3
#http://localhost:8080/generateboxplot?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery
class BoxPlotGeneration():
    def GET(self,r):

        # table_name = web.input().tablename
        # fields = ast.literal_eval(web.input().fields)
        # inputs = [{table_name:fields}]
        inputs = ast.literal_eval(web.input().q)
        dbtype = web.input().dbtype
        id = int(web.input().ID)

        try:
            cache_timeout = int(web.input().t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)
        print ('Request received: Keys: {0}, values: {1}'.format(web.input().keys(), web.input().values()))
        # try:
        result = dp.ret_box(dbtype, inputs, id, cache_timeout)
        #result_ = BP.ret_data(inputs)
            # result = cmg.format_response(True,result_,'Data successfully processed!')
        # except:
        #     logger.error("Error retrieving data from boxplot lib")
        #     result = cmg.format_response(False,None,'Error occurred while getting data from Box plot lib!',sys.exc_info())
        #     raise
        # finally:
        return result

#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery&ID=11
#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&SecurityToken=7749e9d64eea8acf84bc3ee4368cec95&Domain=duosoftware.com
class HistogramGeneration():
    def GET(self,r):

        inputs = ast.literal_eval(web.input().q)
        dbtype = web.input().dbtype
        id = int(web.input().ID)

        try:
            cache_timeout = int(web.input().t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)

        print ('Request received: Keys: {0}, values: {1}'.format(web.input().keys(), web.input().values()))
        #try:
        result = dp.ret_hist(dbtype, inputs, id, cache_timeout)
            #result = cmg.format_response(True,result_,'Data successfully processed!')
        # except:
        #     logger.error("Error retrieving data from histogram lib")
        #     result = cmg.format_response(False,None,'Error occurred while getting data from histogram lib!',sys.exc_info())
        #     raise
        # finally:
        return result

#http://localhost:8080/generatebubble?dbtype=BigQuery&db=Demo&table=humanresource&x=salary&y=Petrol_Allowance&s=salary&c=gender&ID=3
#http://localhost:8080/bubblechart?dbtype=BigQuery&db=Demo&table=humanresource&x=salary&y=Petrol_Allowance&s=salary&c=gender
class BubbleChart():
    def GET(self,r):

        dbtype = web.input().dbtype
        db = web.input().db
        table = web.input().table
        id = int(web.input().ID)
        x = web.input().x
        y = web.input().y
        s = web.input().s
        c = web.input().c

        try:
            cache_timeout = int(web.input().t)
        except AttributeError, err:
            cache_timeout = int(default_cache_timeout)

        print ('Request received: Keys: {0}, values: {1}'.format(web.input().keys(), web.input().values()))
        result = dp.ret_bubble(dbtype, db, table, x, y, s, c, id, cache_timeout)

        return result

if __name__ == "__main__":
    app.run()