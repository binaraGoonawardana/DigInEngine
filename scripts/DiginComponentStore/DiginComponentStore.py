__author__ = 'Marlon Abeykoon'
__version__ = '1.1.1'

import logging
import os, sys
import datetime
import decimal
import random
from operator import itemgetter
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
logger.addHandler(handler)

logger.info('Starting log')

def store_components(params):
         data = json.loads(params)
         print data
         DiginCompDate = datetime.datetime.now()
         #Namespace = data["namespace"]
         #Tenant = data["tenant"]
         Namespace = 'ww'
         Tenant = '3'
         DiginCompName = data["compName"]
         DiginCompClass = data["compClass"]
         DiginCompType = data["compType"]
         DiginCompCategory = data["compCategory"]
         RefreshInterval = data["refreshInterval"]
         pages = data["pages"]

         epoch = datetime.datetime.utcfromtimestamp(0)
         def unix_time_millis(dt):
            return (dt - epoch).total_seconds() * 1000.0

         if data["compID"] is None:
             #DiginCompID = int(random.randint(1000000000000,8000000000000))
             DiginCompID = int(unix_time_millis(datetime.datetime.now()))
             print DiginCompID
             try:
                CC.insert_data([{ 'digin_comp_id':DiginCompID,'digin_comp_name':DiginCompName,'created_date_time':DiginCompDate,'tenant':Tenant,'digin_comp_class':DiginCompClass,
                                  'digin_comp_type':DiginCompType,'digin_comp_category':DiginCompCategory, 'refresh_interval':RefreshInterval,
                                  'namespace': Namespace, 'modified_date_time': DiginCompDate}], 'digin_componentheader')
                logger.info("DigIn Component Successfully created")
             except Exception, err:
                logger.error("Error in updating cache. %s" % err)
                pass

             for page in pages:
                 #page_id = int(random.randint(1000000000000,8000000000000))
                 page_id = int(unix_time_millis(datetime.datetime.now()))
                 try:
                     CC.insert_data([{'page_id':page_id, 'digin_comp_id':DiginCompID,'page_name':page["pageName"],
                                      'page_data': json.dumps(page['pageData']),'namespace': Namespace,'tenant':Tenant }],
                                    'digin_component_page_detail')
                     logger.info("Component page successfully saved!")
                 except Exception, err:
                     logger.error("Error in updating cache. %s" % err)
                     pass
                 widgets = page["widgets"]
                 for widget in widgets:
                     # widget_id = int(random.randint(1000000000000,8000000000000))
                     widget_id = int(unix_time_millis(datetime.datetime.now()))
                     widgetData = widget["widgetData"]
                     print widgetData
                     try:
                        CC.insert_data([{ 'widget_id':widget_id, 'widget_name': widget['widgetName'], 'widget_data':json.dumps(widgetData),
                                          'widget_type': widget['widgetType'], 'digin_comp_id':DiginCompID, 'version_id':1,'tenant':Tenant,
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
                               digin_comp_name=DiginCompName,
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
                                        page_data=json.dumps(page['pageData']))
                     else:
                         #page_id = int(random.randint(1000000000000,8000000000000))
                         page_id = int(unix_time_millis(datetime.datetime.now()))
                         CC.insert_data([{'page_id':page_id, 'digin_comp_id':data["compID"],'page_name':page["pageName"],
                                          'page_data': json.dumps(page['pageData']),'namespace': Namespace,'tenant':Tenant }],
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
                            widget_data=json.dumps(widget['widgetData']),
                            widget_type=widget['widgetType'],
                            version_id=None)
                        else:
                            #widget_id = int(random.randint(1000000000000,8000000000000))
                            widget_id = int(unix_time_millis(datetime.datetime.now()))
                            CC.insert_data([{ 'widget_id':widget_id, 'widget_name': widget['widgetName'], 'widget_data':json.dumps(widgetData),
                                              'widget_type': widget['widgetType'],'digin_comp_id':data["compID"], 'version_id':1,'tenant':Tenant,
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
        data = CC.get_data("SELECT digin_comp_id, digin_comp_name  FROM digin_componentheader")
        print data["rows"]
        comps = []
        for comp in data["rows"]:
            comp_dict ={}
            comp_dict["compID"]= comp[0]
            comp_dict["compName"]=comp[1]
            comps.append(comp_dict)
        return cmg.format_response(True,comps,"Successful!")

      except Exception, err:
        logger.error("Error getting data from cache. %s" % err)
        return cmg.format_response(False,0,"Error Occurred!", exception = sys.exc_info())

def get_component_by_comp_id(params):
      web.header('Access-Control-Allow-Origin',      '*')
      web.header('Access-Control-Allow-Credentials', 'true')
      try:
        data = CC.get_data("SELECT h.digin_comp_id, h.digin_comp_name, h.refresh_interval, h.digin_comp_class, "
                           "h.digin_comp_type, h.digin_comp_category, h.created_date_time, p.page_id, p.page_name, "
                           "p.page_data, d.widget_id, d.widget_name, d.widget_data, d.widget_type "
                           "FROM digin_componentheader h "
                           "LEFT JOIN digin_component_page_detail p "
                           "ON h.digin_comp_id = p.digin_comp_id "
                           "LEFT JOIN digin_componentdetail d "
                           "ON h.digin_comp_id = d.digin_comp_id AND p.page_id = d.comp_page_id "
                           "WHERE h.digin_comp_id = {0} "
                           "ORDER BY d.widget_id ASC".format(params.comp_id))

        print data
        component = {}
        component['compID'] = data['rows'][0][0]
        component['compName'] = data['rows'][0][1]
        component['compClass'] = data['rows'][0][3]
        component['compType'] = data['rows'][0][4]
        component['compCategory'] = data['rows'][0][5]
        component['refreshInterval'] = data['rows'][0][2]

        def json_encode_decimal(obj):
            if isinstance(obj, decimal.Decimal):
                return str(obj)
            raise TypeError(repr(obj) + " is not JSON serializable")

        widget_ids = set(d[10] for d in data['rows'])
        widgets = []
        for widget_id in widget_ids:
            for record in data['rows']:
                if widget_id == record[10]:
                    widget = {
                        'pageID':record[7],
                        'widgetID' : record[10],
                        'widgetName':record[11],
                        'widgetData':json.loads(record[12]) if record[12] is not None else None,
                        'widgetType':record[13]
                    }
                    widgets.append(widget)
        print widgets

        pages = []
        page_ids = set( d[7] for d in data['rows'])
        for page_id in page_ids:
            for record in data['rows']:
                if page_id == record[7]:
                    page ={
                        'pageID': record[7],
                        'pageName': record[8],
                        'pageData': json.loads(record[9]) if record[9] is not None else None,
                        'widgets': []
                    }
                    pages.append(page)
                    break

        pages_sorted = sorted(pages, key=itemgetter('pageID'))
        widgets_sorted = sorted(widgets, key=itemgetter('widgetID'))
        for page in pages_sorted:
            for widget_ in widgets_sorted:
                if widget_['pageID'] == page['pageID']:
                    #widget_.pop("pageID", None)
                    page['widgets'].append(widget_)

        component['pages'] = pages_sorted
        print json.dumps(component,default=json_encode_decimal)

        return cmg.format_response(True,component,"Successful!")

      except Exception, err:
        logger.error("Error getting data from cache. %s" % err)
        return cmg.format_response(False,0,"Error Occurred!", exception = sys.exc_info())

