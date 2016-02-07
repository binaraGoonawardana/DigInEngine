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
            result = BP.ret_data(inputs)
        except:
            logger.error("Error retrieving data from boxplot lib")
        return result

#http://localhost:8080/generatehist?q=[{%27[digin_hnb.humanresource]%27:[%27age%27]}]
class HistogramGeneration():
    def GET(self,r):

        inputs = ast.literal_eval(web.input().q)
        result = ''
        logger.info("Input received HistogramGeneration %s" %inputs)
        logger.info("getting data from Histogram.py")
        try:
            result = Hist.ret_data(inputs)
        except:
            logger.error("Error retrieving data from histogram lib")
        return result

if  __name__ == "__main__":
    app.run()