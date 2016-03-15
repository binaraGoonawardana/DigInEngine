__author__ = 'Marlon'

import sys,os
sys.path.append("...")
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
import modules.Boxplot as BP
import modules.Histogram as Hist
import modules.CommonMessageGenerator as cmg
import DA_getdata as dg
import scripts.DigINCacheEngine.CacheController as CC
import web
import logging
import operator
import ast
import json
import modules.Bubblechart as bbc

urls = (
    '/generateboxplot(.*)', 'BoxPlotGeneration',
    '/generatehist(.*)', 'HistogramGeneration',
    '/bubblechart(.*)', 'bubblechart'
)

app = web.application(urls, globals())

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('DescriptiveAnalytics.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')

#http://localhost:8080/generateboxplot?q=[{%27[digin_hnb.humanresource]%27:[%27age%27,%27salary%27]}]
class BoxPlotGeneration():
    def GET(self,r):

        table_name = web.input().tablename
        fields = ast.literal_eval(web.input().fields)
        inputs = [{table_name:fields}]
        result = ''
        logger.info("Input received BoxPlotGeneration %s" %inputs)
        logger.info("getting data from bloxplot.py")
        try:
            result_ = BP.ret_data(inputs)
            result = cmg.format_response(True,result_,'Data successfully processed!')
        except:
            logger.error("Error retrieving data from boxplot lib")
            result = cmg.format_response(False,None,'Error occurred while getting data from Box plot lib!',sys.exc_info())
            raise
        finally:
            return result

#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27age%27]}]
class HistogramGeneration():
    def GET(self,r):

        inputs = ast.literal_eval(web.input().q)
        result = ''
        #dbtype = web.input().dbtype
        logger.info("Input received HistogramGeneration %s" %inputs)
        logger.info("getting data from Histogram.py")
        try:
            result_ = Hist.ret_data(inputs)
            result = cmg.format_response(True,result_,'Data successfully processed!')
        except:
            logger.error("Error retrieving data from histogram lib")
            result = cmg.format_response(False,None,'Error occurred while getting data from histogram lib!',sys.exc_info())
            raise
        finally:
            return result

#http://localhost:8080/bubblechart?dbtype=BigQuery&db=Demo&table=humanresource&x=salary&y=Petrol_Alllowance&s=salary&c=gender
class bubblechart():
    def GET(self,r):

        dbtype = web.input().dbtype
        db = web.input().db
        table = web.input().table
        x = web.input().x
        y = web.input().y
        s = web.input().s
        c = web.input().c

        result = bbc.bubblechart(dbtype, db, table, x, y, s, c)

        return result

if __name__ == "__main__":
    app.run()