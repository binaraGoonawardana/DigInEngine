__author__ = 'Marlon Abeykoon'
__version__ = '1.1.0.1'

import scripts.DigINCacheEngine.CacheController as CC
import modules.CommonMessageGenerator as cmg
import datetime
import logging
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

    logo_path = conf.get_conf('FilePathConfig.ini','User Files')['Path']+'/'+user_id+'/logos'
    data_object = [{'user_id': user_id,
             'email': params['email'],
             'components': params['components'],
             'user_role': params['user_role'],
             'cache_lifetime': int(params['cache_lifetime']),
             'widget_limit': int(params['widget_limit']),
             'query_limit': int(params['query_limit']),
             'logo_path': logo_path+'/'+params['logo_name'],
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
                           logo_path=logo_path+'/'+['logo_name'],
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
    query = "SELECT * FROM digin_user_settings WHERE user_id = '{0}' AND domain = '{1}'".format(user_id, domain)
    try:
        user_data = CC.get_data(query)

        data ={
             'components': user_data['rows'][0][2],
             'user_role': user_data['rows'][0][3],
             'cache_lifetime': int(user_data['rows'][0][4]),
             'widget_limit': int(user_data['rows'][0][5]),
             'query_limit': int(user_data['rows'][0][6]),
             'logo_path': user_data['rows'][0][7],
             'theme_config': user_data['rows'][0][8],
             'modified_date_time': datetime.datetime.now(),
             'created_date_time': datetime.datetime.now(),
             'domain': domain
             }

    except Exception, err:
        logger.error("Error retrieving user settings")
        logger.error(err)
        print "Error retrieving user settings"
        print err
        raise
    return cmg.format_response(True,data,"User settings retrieved successfully")
