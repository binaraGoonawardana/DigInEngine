__author__ = 'Marlon Abeykoon'
__version__ = '1.1.0.6'

import scripts.DigINCacheEngine.CacheController as CC
import modules.BigQueryHandler as bq
import modules.CommonMessageGenerator as cmg
import scripts.PentahoReportingService as prs
import sys
import datetime
import logging
import re
import ast
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/UserManagementService.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  UserManagementService  ------------------------------------------')
logger.info('Starting log')

def store_user_settings(params,user_id, domain):

    data_object = [{'user_id': user_id,
             'email': params['email'],
             'components': params['components'],
             'user_role': params['user_role'],
             'cache_lifetime': int(params['cache_lifetime']),
             'widget_limit': int(params['widget_limit']),
             'query_limit': int(params['query_limit']),
             'logo_path': '/digin_user_data/'+user_id+'/'+domain+'/logos/'+params['logo_name'],
             'dp_path': '/digin_user_data/'+user_id+'/'+domain+'/DPs/'+params['dp_name'],
             'theme_config': params['theme_config'],
             'modified_date_time': datetime.datetime.now(),
             'created_date_time': datetime.datetime.now(),
             'domain': domain
             }]
    logger.info("Data received!")
    logger.info(data_object)
    existance = CC.get_data("SELECT user_id from digin_user_settings where user_id = '{0}' AND domain = '{1}'".format(user_id, domain))
    if existance['rows'] != ():
        try:
            CC.update_data('digin_user_settings',"WHERE user_id='{0}'".format(user_id),
                           components=params['components'],
                           user_role=params['user_role'],
                           cache_lifetime=int(params['cache_lifetime']),
                           widget_limit=int(params['widget_limit']),
                           query_limit=int(params['query_limit']),
                           logo_path='/digin_user_data/'+user_id+'/'+domain+'/logos/'+params['logo_name'],
                           dp_path='/digin_user_data/'+user_id+'/'+domain+'/DPs/'+params['dp_name'],
                           theme_config=params['theme_config'],
                           modified_date_time=datetime.datetime.now())
            return cmg.format_response(True,1,"User settings updated successfully")
        except Exception, err:
            logger.error("Error updating user settings")
            logger.error(err)
            print "Error updating user settings"
            print err
            raise
    try:
        CC.insert_data(data_object,'digin_user_settings')
    except Exception, err:
        logger.error("Error saving user settings")
        logger.error(err)
        print "Error saving user settings"
        print err
        raise
    return cmg.format_response(True,1,"User settings saved successfully")

def get_user_settings(user_id, domain):

    query = "SELECT components, user_role, cache_lifetime, widget_limit, " \
            "query_limit, logo_path, dp_path, theme_config, modified_date_time, created_date_time, " \
            "domain FROM digin_user_settings WHERE user_id = '{0}' AND domain = '{1}'".format(user_id, domain)
    logo_path = conf.get_conf('FilePathConfig.ini','User Files')['Path']
    document_root = conf.get_conf('FilePathConfig.ini','Document Root')['Path']
    path = re.sub(document_root, '', logo_path)
    try:
        user_data = CC.get_data(query)
        if user_data['rows'] == ():
            logger.info('No user settings saved for given user ' + user_id)
            return cmg.format_response(True,user_id,"No user settings saved for given user and domain")
        data ={
             'components': user_data['rows'][0][0],
             'user_role': user_data['rows'][0][1],
             'cache_lifetime': int(user_data['rows'][0][2]),
             'widget_limit': int(user_data['rows'][0][3]),
             'query_limit': int(user_data['rows'][0][4]),
             'logo_path': path+user_data['rows'][0][5],
             'dp_path': path+user_data['rows'][0][6],
             'theme_config': user_data['rows'][0][7],
             'modified_date_time': user_data['rows'][0][8],
             'created_date_time': user_data['rows'][0][9],
             'domain': user_data['rows'][0][10]
             }

    except Exception, err:
        logger.error("Error retrieving user settings")
        logger.error(err)
        print "Error retrieving user settings"
        print err
        raise
    return cmg.format_response(True,data,"User settings retrieved successfully")

def set_initial_user_env(params,email,user_id,domain):

    default_user_settings = conf.get_conf('DefaultConfigurations.ini','User Settings')
    default_sys_settings = conf.get_conf('DefaultConfigurations.ini','System Settings')
    dataset_name = email.replace(".", "_").replace("@","_")

    if ast.literal_eval(default_sys_settings['signup_dataset_creation']):
        db = params['db']
        if db.lower() == 'bigquery':
            logger.info("Creation of dataset started!")
            print "Creation of dataset started!"
            try:
                result_ds= bq.create_dataset(dataset_name)
                print result_ds
                logger.info("Creation of dataset status " + str(result_ds))
                print "Creation of dataset " + str(result_ds)
            except Exception, err:
              print err
              print "Creation of dataset failed!"
              return cmg.format_response(False,err,"Error Occurred while creating dataset in bigquery!",exception=sys.exc_info())
        else:
            raise

    default_data = {
             'email': email,
             'components': None,
             'user_role': None,
             'cache_lifetime': 300,
             'widget_limit': default_user_settings['widget_limit'],
             'query_limit': default_user_settings['query_limit'],
             'logo_name': default_user_settings['logo_name'],
             'dp_name': default_user_settings['dp_name'],
             'theme_config': default_user_settings['theme_conf'],
             'modified_date_time': datetime.datetime.now(),
             'created_date_time': datetime.datetime.now()
             }
    try:
        result_us = store_user_settings(default_data, user_id, domain)
        print result_us
    except Exception, err:
        print err
        logger.error(err)
        return cmg.format_response(False,err,"Error Occurred while applying initial user settings!",exception=sys.exc_info())

    logger.info("Initial user settings applied!")
    print "Initial user settings applied!"

    if ast.literal_eval(default_sys_settings['signup_sample_dashboards']):

        logger.info("User will be given the default dashboard access!")
        print "User will be given the default dashboard access!"
        try:
            access_detail_obj = []
            for component in ast.literal_eval(default_user_settings['components']):
                access_detail_comp = {'user_id':user_id,
                                      'component_id':component,
                                      'type':'dashboard',
                                      'domain':domain}
                access_detail_obj.append(access_detail_comp)
            result_ad = CC.insert_data(access_detail_obj,'digin_component_access_details')
            print result_ad
        except Exception, err:
            print err
            return cmg.format_response(False,err,"Error Occurred while giving default dashboard access",exception=sys.exc_info())
    #TODO insert menuids to access_details based on user's package
    if ast.literal_eval(default_sys_settings['signup_sample_reports']):
        logger.info("User will be given the default report access!")
        print "User will be given the default report access!"
        try:
            prs.ReportInitialConfig.prptConfig(user_id,domain)
            prs.ReportInitialConfig.ktrConfig(user_id,domain)

        except Exception, err:
            print err
            return cmg.format_response(False,err,"Error Occurred while giving default report access",exception=sys.exc_info())

    return cmg.format_response(True,1,"User environment initialized successfully")

