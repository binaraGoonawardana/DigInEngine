__author__ = 'Marlon Abeykoon'
__version__ = '1.1.6'

import logging
import os, sys
import datetime
import decimal
from operator import itemgetter
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import modules.CommonMessageGenerator as cmg
import scripts.DigINCacheEngine.CacheController as CC
import scripts.DigInRatingEngine.DigInRatingEngine as dre
import json
import pprint
import threading
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

def _rate_calculation_helper(is_increment,comp_id,type,user_id,tenant):
        usages = {type: 1}
        obj = dre.RatingEngine(user_id, tenant, comp_id, is_increment, **usages)
        p3 = threading.Thread(target=obj.set_usage(), args=())
        p3.start()

def store_components(params, user_id, domain):
         data = json.loads(params)
         pprint.pprint(data)
         DiginCompDate = datetime.datetime.now()
         User_id = user_id
         Domain = domain
         DiginCompName = data["compName"]
         DiginCompClass = data["compClass"]
         DiginCompType = data["compType"]
         DiginCompCategory = data["compCategory"]
         RefreshInterval = data["refreshInterval"]
         pages = data["pages"]
         deletions = data["deletions"]

         epoch = datetime.datetime.utcfromtimestamp(0)
         def unix_time_millis(dt):
            return (dt - epoch).total_seconds() * 1000.0

         if data["compID"] is None:
             DiginCompID = int(unix_time_millis(datetime.datetime.now()))
             print DiginCompID
             try:
                p1 = threading.Thread(target=CC.insert_data,args=([{ 'digin_comp_id':DiginCompID,
                                                                     'digin_comp_name':DiginCompName,
                                                                     'created_date_time':DiginCompDate,
                                                                     'digin_comp_class':DiginCompClass,
                                                                     'digin_comp_type':DiginCompType,
                                                                     'digin_comp_category':DiginCompCategory,
                                                                     'refresh_interval':RefreshInterval,
                                                                     'created_user': User_id,
                                                                     'created_tenant':Domain,
                                                                     'modified_date_time':DiginCompDate,
                                                                     'last_modified_user':User_id}],
                                                                  'digin_component_header'))
                p2 = threading.Thread(target=CC.insert_data, args=([{ 'component_id':DiginCompID,'user_id':User_id,
                                                                      'type':'dashboard','domain':Domain}],
                                                                   'digin_component_access_details'))
                p1.start()
                p2.start()
                p1.join()
                p2.join()
                _rate_calculation_helper(True,DiginCompID,DiginCompType,User_id,Domain)
                logger.info("DigIn Component Successfully created")
             except Exception, err:
                logger.error("Error in updating cache. %s" % err)
                return cmg.format_response(False,None,'Error occurred while saving dashboard!',sys.exc_info())

             for page in pages:
                 page_id = int(unix_time_millis(datetime.datetime.now()))
                 try:
                     CC.insert_data([{'page_id':page_id, 'digin_comp_id':DiginCompID,'page_name':page["pageName"],
                                      'page_data': json.dumps(page['pageData'])}],
                                    'digin_component_page_detail')
                     logger.info("Component page successfully saved!")
                 except Exception, err:
                     logger.error("Error in updating cache. %s" % err)
                     return cmg.format_response(False,None,'Error occurred while saving dashboard!',sys.exc_info())
                 widgets = page["widgets"]
                 for widget in widgets:
                     widget_id = int(unix_time_millis(datetime.datetime.now()))
                     widgetData = widget["widgetData"]
                     print widgetData
                     try:
                        CC.insert_data([{ 'widget_id':widget_id, 'widget_name': widget['widgetName'], 'widget_data':json.dumps(widgetData),
                                          'digin_comp_id':DiginCompID, 'version_id':1,'size_x':widget['sizeX'],'size_y':widget['sizeY'],'row':widget['row'],'col':widget['col'],
                                          'comp_page_id': page_id}], 'digin_component_detail')
                        logger.info("Digin Widget Successfully created")
                     except Exception, err:
                        logger.error("Error in updating cache. %s" % err)
                        return cmg.format_response(False,None,'Error occurred while saving dashboard!',sys.exc_info())
             logger.info("Dashboard saving successful!")
             print "Dashboard saving successful!"
             return cmg.format_response(True,DiginCompID,"Successfully saved!")
         else:
             try:
                CC.update_data('digin_component_header','WHERE digin_comp_id ={0}'.format(data["compID"]),
                               modified_date_time=datetime.datetime.now(),
                               digin_comp_name=DiginCompName,
                               digin_comp_class=DiginCompClass,
                               digin_comp_type=DiginCompType,
                               digin_comp_category=DiginCompCategory,
                               refresh_interval=RefreshInterval,
                               last_modified_user=User_id
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
                         page_id = int(unix_time_millis(datetime.datetime.now()))
                         CC.insert_data([{'page_id':page_id, 'digin_comp_id':data["compID"],'page_name':page["pageName"],
                                          'page_data': json.dumps(page['pageData']) }],
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
                            CC.update_data('digin_component_detail','WHERE digin_comp_id ={0} AND comp_page_id ={1} AND widget_id ={2}'
                            .format(data["compID"],page['pageID'],widget['widgetID']),
                            widget_name=widget['widgetName'],
                            widget_data=json.dumps(widget['widgetData']),
                            size_x = widget['sizeX'],
                            size_y = widget['sizeY'],
                            row = widget['row'],
                            col = widget['col'],
                            version_id=None)
                        else:
                            widget_id = int(unix_time_millis(datetime.datetime.now()))
                            CC.insert_data([{ 'widget_id':widget_id, 'widget_name': widget['widgetName'], 'widget_data':json.dumps(widgetData),
                                              'digin_comp_id':data["compID"], 'version_id':1,'size_x':widget['sizeX'],'size_y':widget['sizeY'],'row':widget['row'],'col':widget['col'],
                                              'comp_page_id': page['pageID'] or page_id, }], 'digin_component_detail')
                        logger.info("Digin Widget Successfuly created")
                     except Exception, err:
                        logger.error("Error in updating cache. %s" % err)
                        raise

             for dashboard in deletions["componentIDs"]:
                 CC.update_data('digin_component_header','WHERE digin_comp_id = {0}'.format(dashboard),
                               is_active=False)

             for page in deletions["pageIDs"]:
                 CC.update_data('digin_component_page_detail','WHERE page_id = {0}'.format(page),
                               is_active=False)

             for widget in deletions["widgetIDs"]:
                 CC.update_data('digin_component_detail','WHERE widget_id = {0}'.format(widget),
                               is_active=False)

             logger.info("Dashboard updated successfully!")
             print "Dashboard updated successfully!"
             return cmg.format_response(True,data["compID"],"Successfully updated!")


def get_all_components(params, user_id, domain):
      try:
        data = CC.get_data("SELECT h.digin_comp_id, h.digin_comp_name, h.digin_comp_type "
                           "FROM digin_component_access_details a "
                           "INNER JOIN digin_component_header h "
                           "ON a.component_id = h.digin_comp_id "
                           "WHERE h.is_active = TRUE AND a.domain = '{0}' AND a.user_id = '{1}' "
                           "AND a.type IN ('dashboard','report')".format(domain, user_id))
        print data["rows"]
        comps = []
        for comp in data["rows"]:
            comp_dict ={}
            comp_dict["compID"]= comp[0]
            comp_dict["compName"]=comp[1]
            comp_dict["compType"]=comp[2]
            comps.append(comp_dict)
        return cmg.format_response(True,comps,"Successful!")

      except Exception, err:
        logger.error("Error getting data from cache. %s" % err)
        return cmg.format_response(False,0,"Error Occurred!", exception = sys.exc_info())

def get_component_by_comp_id(params, user_id, domain):
      try:
        data = CC.get_data("SELECT h.digin_comp_id, h.digin_comp_name, h.refresh_interval, h.digin_comp_class, "
                           "h.digin_comp_type, h.digin_comp_category, h.created_date_time, p.page_id, p.page_name, "
                           "p.page_data, d.widget_id, d.widget_name, d.widget_data,d.row,d.col,d.size_x,d.size_y "
                            "FROM "
                            "(SELECT component_id FROM digin_component_access_details WHERE "
                            "component_id = {0} AND domain = '{1}' AND user_id = '{2}') a "
                            "INNER JOIN " # INNER join is to get only the components in component header table i.e: dashboards, reports
                            "(SELECT digin_comp_id, digin_comp_name, refresh_interval, digin_comp_class, "
                            "digin_comp_type, digin_comp_category, created_date_time "
                            "FROM digin_component_header "
                            "WHERE is_active = TRUE AND digin_comp_id = {0}) h "
                            "ON a.component_id = h.digin_comp_id "
                            "LEFT JOIN "
                            "(SELECT page_id, page_name, page_data, digin_comp_id  FROM digin_component_page_detail "
                            "WHERE is_active = TRUE) p "
                            "ON h.digin_comp_id = p.digin_comp_id "
                            "LEFT JOIN "
                            "(SELECT widget_id, widget_name, widget_data,size_x,size_y,col,row, digin_comp_id, comp_page_id  FROM digin_component_detail "
                           "WHERE is_active = TRUE) d "
                           "ON h.digin_comp_id = d.digin_comp_id AND p.page_id = d.comp_page_id "
                           "ORDER BY d.widget_id ASC".format(params.comp_id, domain, user_id))

        if data['rows'] == ():
            print "No dashboard saved for given ID!"
            return cmg.format_response(True,None,"No dashboard saved for given ID!")
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
        #filter(None, widget_ids[0])
        for widget_id in widget_ids:
            if widget_id is None:
                continue
            for record in data['rows']:
                if widget_id == record[10]:
                    widget = {
                        'pageID':record[7],
                        'widgetID' : record[10],
                        'sizeX':record[15],
                        'sizeY':record[16],
                        'row':record[13],
                        'col':record[14],
                        'widgetName':record[11],
                        'widgetData':json.loads(record[12]) if record[12] is not None else None
                    }
                    widgets.append(widget)

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

        return cmg.format_response(True,component,"Successful!")

      except Exception, err:
        logger.error("Error getting data from cache. %s" % err)
        return cmg.format_response(False,0,"Error Occurred!", exception = sys.exc_info())

def _permanent_delete_components(comp_id, table, user_id, domain): #TODO query not working
        try:
            result = CC.delete_data("DELETE a FROM {0} a INNER JOIN digin_component_access_details b "
                                    "ON a.digin_comp_id = b.component_id WHERE digin_comp_id = {1} AND domain = '{2}' AND "
                                    "user_id = '{3}' ".format(table, comp_id,domain,user_id))
            print 'Component permanently DELETED: %s ' %str(comp_id)
        except Exception, err:
            print err
            raise
        return result

def _temporary_delete_components(comp_id, table, user_id, domain):
        try:
            is_access_valid = CC.get_data("SELECT digin_comp_id FROM digin_component_header a INNER JOIN "
                                          "digin_component_access_details b ON a.digin_comp_id = b.component_id "
                                          "WHERE digin_comp_id = {0} AND domain = '{1}' AND user_id = '{2}'".format(comp_id,domain,user_id))
            if int(is_access_valid['rows'][0][0]) == int(comp_id):
                result = CC.update_data(table,'WHERE digin_comp_id = {0}'.format(comp_id),is_active=False)
                _rate_calculation_helper(False, comp_id, 'dashboard', user_id, domain) # Component count is decremented when components moved to Trash
                print 'Component deleted user_id: {0}, tenant: {1}, component_id: {2}'.format(user_id, domain, comp_id)
            else:
                result = 'No component found for deletion user_id: {0}, tenant: {1}, component_id: {2}'.format(user_id,domain,comp_id)
                print result
                return result
        except Exception, err:
            print err
            raise
        return result

def delete_component(params, user_id, domain):
    for comp in params:
        print "Deleting component %s ..." % comp
        if comp['permanent_delete']:
            try:
                    p1 = threading.Thread(target=_permanent_delete_components,args=(comp['comp_id'],'digin_component_header',
                                                                                          user_id, domain))

                    p2 = threading.Thread(target=_permanent_delete_components,args=(comp['comp_id'],'digin_component_page_detail',
                                                                                          user_id, domain))

                    p3 = threading.Thread(target=_permanent_delete_components,args=(comp['comp_id'],'digin_component_detail',
                                                                                          user_id, domain))
                    p1.start()
                    p2.start()
                    p3.start()
                    p1.join()
                    p2.join()
                    p3.join()
            except Exception, err:
                    print err
                    logger.error("Permanent deletion failed. %s" % err)
                    return cmg.format_response(False,0,"Error Occurred in deletion!", exception = sys.exc_info())


        else:
            try:
                    p1 = threading.Thread(target=_temporary_delete_components,args=(comp['comp_id'],'digin_component_header',
                                                                                          user_id, domain))

                    p2 = threading.Thread(target=_temporary_delete_components,args=(comp['comp_id'],'digin_component_page_detail',
                                                                                          user_id, domain))

                    p3 = threading.Thread(target=_temporary_delete_components,args=(comp['comp_id'],'digin_component_detail',
                                                                                          user_id, domain))
                    p1.start()
                    p2.start()
                    p3.start()
                    p1.join()
                    p2.join()
                    p3.join()
            except Exception, err:
                    print err
                    logger.error("Temporary deletion failed. %s" % err)
                    return cmg.format_response(False,0,"Error Occurred in deletion!", exception = sys.exc_info())

    print "Components deleted!"
    return cmg.format_response(True,0,"Components deleted!")


