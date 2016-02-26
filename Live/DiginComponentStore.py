__author__ = 'Sajeetharan'
import logging
import os, sys
import datetime as dt
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
import ConfigHandler as conf
import AuthHandler as Auth
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
from bigquery import get_client
try:
    import  CacheController as CC
except:
    pass
import json
import web
import random
import urllib
from bigquery import get_client
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('DiginComponent.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
urls = (
    '/storeComponent(.*)', 'store_Component',
    '/GetAllComponent(.*)', 'get_Allcomponent',
    '/GetComponentByCategory(.*)', 'get_ComponentByCategory',
    '/GetComponentById(.*)', 'get_ComponentById'
)
app = web.application(urls, globals())
class store_Component:
    def POST(self,r):
         data = json.loads(web.data())
         widgets = []
         DiginCompDate = dt.datetime.today().strftime("%m/%d/%Y")
         VersionID = 1
         Namespace = data["Namespace"]
         LastUdated = dt.datetime.today().strftime("%m%d%Y")
         Tenant = data["Tenant"]
         DiginCompClass = data["DiginCompClass"]
         DiginCompType = data["DiginCompType"]
         DiginCompCategory = data["DiginCompCategory"]
         widgets = data["widgets"]
         try:
            CC.insert_data([{ 'DiginCompDate':DiginCompDate,'Tenant':Tenant,'DiginCompClass':DiginCompClass,
                              'DiginCompType':DiginCompType,'DiginCompCategory':DiginCompCategory,
                              'Namespace': Namespace, 'LastUdated': DiginCompDate}], 'digin_componentheader')
            logger.info("Digin Component Successfuly created")
         except Exception, err:
            logger.error("Error in updating cache. %s" % err)
         pass
         i =0
         for widget in widgets:
             i += 1
             widgetData = widget["DiginCompWidgetData"]
             try:
                CC.insert_data([{ 'DiginCompWidgetID':i,'DiginCompWidgetData':widgetData,'DiginCompID':1,
                                  'VersionID':1,'Tenant':Tenant,
                                  'Namespace': Namespace, 'LastUdated': DiginCompDate}], 'digin_componentheader')
                logger.info("Digin Widget Successfuly created")
             except Exception, err:
                logger.error("Error in updating cache. %s" % err)
             pass

if __name__ == "__main__":
    app.run()
