__author__ = 'Sajeetharan'
import logging
import os, sys
import datetime as dt

import random
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
# import ConfigHandler as conf
# import AuthHandler as Auth
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import modules.CommonMessageGenerator as cmg
from bigquery import get_client
import  scripts.DigINCacheEngine.CacheController as CC
import json
import web
import configs.ConfigHandler as conf

import urllib
from bigquery import get_client
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/DiginComponent.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

def store_components(params):
         data = json.loads(params)
         widgets = []
         DiginCompDate = dt.datetime.today().strftime("%m/%d/%Y")
         VersionID = 1
         DiginCompID = int(random.randint(1000000000000,8000000000000))
         newCompID = DiginCompID
         Namespace = data["Namespace"]
         LastUdated = dt.datetime.today().strftime("%m%d%Y")
         Tenant = data["Tenant"]
         DiginCompClass = data["DiginCompClass"]
         DiginCompType = data["DiginCompType"]
         DiginCompCategory = data["DiginCompCategory"]
         widgets = data["widgets"]
         try:
            CC.insert_data([{ 'DiginCompID':DiginCompID,'DiginCompDate':DiginCompDate,'Tenant':Tenant,'DiginCompClass':DiginCompClass,
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
                CC.insert_data([{ 'DiginCompWidgetID':i,'DiginCompWidgetData':widgetData,'DiginCompID':DiginCompID,
                                  'VersionID':1,'Tenant':Tenant,
                                  'Namespace': Namespace}], 'digin_componentdetail')
                logger.info("Digin Widget Successfuly created")
             except Exception, err:
                logger.error("Error in updating cache. %s" % err)
                raise
         logger.info("Header and detail saving successful!")
         return cmg.format_response(True,1,"Successfully saved!")



def get_all_components(params):
      web.header('Access-Control-Allow-Origin',      '*')
      web.header('Access-Control-Allow-Credentials', 'true')
      try:
        data = CC.get_data("Select * from digin_componentdetail")
        print type(data)
        print data
        return cmg.format_response(True,data,"Successful!")

      except Exception, err:
        logger.error("Error getting data from cache. %s" % err)
        return cmg.format_response(False,0,"Error Occurred!", exception = sys.exc_info())

def get_component_by_comp_id(params):
      web.header('Access-Control-Allow-Origin',      '*')
      web.header('Access-Control-Allow-Credentials', 'true')
      try:
        data = CC.get_data("SELECT * FROM digin_componentheader WHERE DigInCompID = {0}".format(params.comp_id))
        print type(data)
        print data
        return cmg.format_response(True,data,"Successful!")

      except Exception, err:
        logger.error("Error getting data from cache. %s" % err)
        return cmg.format_response(False,0,"Error Occurred!", exception = sys.exc_info())

