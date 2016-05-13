__author__ = 'Marlon Abeykoon'
__version__ = '1.1.0'

import logging
import os, sys
import datetime

import random
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import modules.CommonMessageGenerator as cmg
import  scripts.DigINCacheEngine.CacheController as CC
import json
import web
import configs.ConfigHandler as conf

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
         print data
         DiginCompDate = datetime.datetime.now()
         Namespace = data["namespace"]
         Tenant = data["tenant"]
         DiginCompClass = data["compClass"]
         DiginCompType = data["compType"]
         DiginCompCategory = data["compCategory"]
         RefreshInterval = data["refreshInterval"]
         pages = data["pages"]
         if data["compID"] is None:
             DiginCompID = int(random.randint(1000000000000,8000000000000))
             try:
                CC.insert_data([{ 'digin_comp_id':DiginCompID,'created_date_time':DiginCompDate,'tenant':Tenant,'digin_comp_class':DiginCompClass,
                                  'digin_comp_type':DiginCompType,'digin_comp_category':DiginCompCategory, 'refresh_interval':RefreshInterval,
                                  'namespace': Namespace, 'modified_date_time': DiginCompDate}], 'digin_componentheader')
                logger.info("DigIn Component Successfully created")
             except Exception, err:
                logger.error("Error in updating cache. %s" % err)
                pass

             for page in pages:
                 page_id = int(random.randint(1000000000000,8000000000000))
                 try:
                     CC.insert_data([{'page_id':page_id, 'digin_comp_id':DiginCompID,'page_name':page["pageName"],
                                      'page_data': page['pageData'],'namespace': Namespace,'tenant':Tenant }],
                                    'digin_component_page_detail')
                     logger.info("Component page successfully saved!")
                 except Exception, err:
                     logger.error("Error in updating cache. %s" % err)
                     pass
                 widgets = page["widgets"]
                 for widget in widgets:
                     widget_id = int(random.randint(1000000000000,8000000000000))
                     widgetData = widget["widgetData"]
                     print widgetData
                     try:
                        CC.insert_data([{ 'widget_id':widget_id, 'widget_name': widget['widgetName'], 'widget_data':widgetData,
                                          'digin_comp_id':DiginCompID, 'version_id':1,'tenant':Tenant,
                                          'comp_page_id': page_id, 'namespace': Namespace}], 'digin_componentdetail')
                        logger.info("Digin Widget Successfully created")
                     except Exception, err:
                        logger.error("Error in updating cache. %s" % err)
                        raise
             logger.info("Dashboard saving successful!")
             print "Dashboard saving successful!"
             return cmg.format_response(True,DiginCompID,"Successfully saved!")
         else:
             try:
                CC.update_data('digin_componentheader','WHERE digin_comp_id ={0}'.format(data["compID"]),
                               version_id=None,
                               modified_date_time=datetime.datetime.now(),
                               digin_comp_class=DiginCompClass,
                               digin_comp_type=DiginCompType,
                               digin_comp_category=DiginCompCategory,
                               refresh_interval=RefreshInterval
                               )
                logger.info("Digin component successfully updated")
             except Exception, err:
                logger.error("Error in updating component. %s" % err)
                pass

             for page in pages:
                 try:
                     if page['pageID'] is not None:
                         CC.update_data('digin_component_page_detail','WHERE digin_comp_id ={0} AND page_id ={1}'
                                        .format(data["compID"],page['pageID']),
                                        page_name=page['pageName'],
                                        page_data=page['pageData'])
                     else:
                         page_id = int(random.randint(1000000000000,8000000000000))
                         CC.insert_data([{'page_id':page_id, 'digin_comp_id':data["compID"],'page_name':page["pageName"],
                                          'page_data': page['pageData'],'namespace': Namespace,'tenant':Tenant }],
                                        'digin_component_page_detail')
                     logger.info("Component page updated saved!")
                 except Exception, err:
                     logger.error("Error in updating cache. %s" % err)
                     pass

                 widgets = page["widgets"]
                 for widget in widgets:
                     widgetData = widget["widgetData"]
                     print widgetData
                     try:
                        if widget['widgetID'] is not None:
                            CC.update_data('digin_componentdetail','WHERE digin_comp_id ={0} AND comp_page_id ={1} AND widget_id ={2}'
                            .format(data["compID"],page['pageID'],widget['widgetID']),
                            widget_name=widget['widgetName'],
                            widget_data=widget['widgetData'],
                            version_id=None)
                        else:
                            widget_id = int(random.randint(1000000000000,8000000000000))
                            CC.insert_data([{ 'widget_id':widget_id, 'widget_name': widget['widgetName'], 'widget_data':widgetData,
                                              'digin_comp_id':data["compID"], 'version_id':1,'tenant':Tenant,
                                              'comp_page_id': page['pageID'] or page_id, 'namespace': Namespace}], 'digin_componentdetail')
                        logger.info("Digin Widget Successfuly created")
                     except Exception, err:
                        logger.error("Error in updating cache. %s" % err)
                        raise
                 logger.info("Dashboard updated successfully!")
             print "Dashboard updated successfully!"
             return cmg.format_response(True,data["compID"],"Successfully updated!")




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

