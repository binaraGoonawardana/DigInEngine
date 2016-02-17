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
import modules.boxplot as BP
import modules.Histogram as Hist
import modules.CommonMessageGenerator as cmg
import scripts.DigINCacheEngine.CacheController as CC
import web
import logging
import operator
import ast
import json

urls = (
    '/generateboxplot(.*)', 'BoxPlotGeneration',
    '/generatehist(.*)', 'HistogramGeneration'
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

        inputs = ast.literal_eval(web.input().q)
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

#http://localhost:8080/generatehist?q=[{%27[digin_hnb.humanresource]%27:[%27age%27]}]
class HistogramGeneration():
    def GET(self,r):

        inputs = ast.literal_eval(web.input().q)
        result = ''
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

if  __name__ == "__main__":
    app.run()