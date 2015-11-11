__author__ = 'Marlon'

import sys
sys.path.append("...")
import modules.boxplot as BP
import scripts.DigINCacheEngine.CacheController as CC
import web
import logging
import operator
import ast
import json

urls = (
    '/generateboxplot(.*)', 'BoxPlotGeneration'
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

if  __name__ == "__main__":
    app.run()