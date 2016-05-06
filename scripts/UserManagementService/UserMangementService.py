__author__ = 'Marlon Abeykoon'
__version__ = '1.1.0.0'

import scripts.DigINCacheEngine.CacheController as CC
import modules.CommonMessageGenerator as cmg
import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('UserManagementService.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  UserManagementService  ------------------------------------------')
logger.info('Starting log')

def store_user_settings(params):
    data_object = [{'user_id': params['user_id'],
             'email': params['email'],
             'components': params['components'],
             'user_role': params['user_role'],
             'refresh_interval': int(params['refresh_interval']),
             'widget_limit': int(params['widget_limit']),
             'query_limit': int(params['query_limit']),
             'image_path': params['image_path'],
             'theme_config': params['theme_config'],
             'modified_date_time': datetime.datetime.now(),
             'created_date_time': datetime.datetime.now()
             }]
    logger.info("Data received!")
    logger.info(data_object)
    existance = CC.get_data("SELECT user_id from digin_user_settings where user_id = '{0}'".format(params['user_id']))
    if existance['rows'] != ():
        try:
            CC.update_data('digin_user_settings',"WHERE user_id='{0}'".format(params['user_id']),
                           components=params['components'],
                           user_role=params['user_role'],
                           refresh_interval=int(params['refresh_interval']),
                           widget_limit=int(params['widget_limit']),
                           query_limit=int(params['query_limit']),
                           image_path=params['image_path'],
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

def get_user_settings_by_id(params):
    query = "SELECT * FROM digin_user_settings WHERE user_id = {0}".format(params.user_id)
    try:
        user_data = CC.get_data(query)
    except Exception, err:
        logger.error("Error retrieving user settings")
        logger.error(err)
        print "Error retrieving user settings"
        print err
        raise
    return cmg.format_response(True,user_data,"User settings retrieved successfully")
