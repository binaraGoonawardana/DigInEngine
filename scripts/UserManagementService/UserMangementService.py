__author__ = 'Marlon Abeykoon'

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
             'refresh_interval': params['refresh_interval'],
             'widget_limit': params['widget_limit'],
             'query_limit': params['query_limit'],
             'image_path': params['image_path'],
             'theme_config': params['theme_config'],
             'created_date_time' : datetime.datetime.now()
             }]
    logger.info("Data received!")
    logger.info(data_object)
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
    query = "SELECT * FROM digin_user_settings WHERE user_id = {0}".format(int(params.user_id))
    try:
        user_data = CC.get_data(query)
    except Exception, err:
        logger.error("Error retrieving user settings")
        logger.error(err)
        print "Error retrieving user settings"
        print err
        raise
    return cmg.format_response(True,user_data,"User settings saved successfully")
