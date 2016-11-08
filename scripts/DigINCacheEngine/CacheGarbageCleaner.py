__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import datetime
import time
import sys
import ast
import logging
sys.path.append("...")
import CacheController as CC
import configs.ConfigHandler as conf

caching_tables = conf.get_conf('CacheConfig.ini', 'Caching Tables')
tables = caching_tables['table_names']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/CacheGarbageCleaner.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')
datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
cache_cleaning_interval = float(datasource_settings['cache_cleaning_interval'])
cache_state_conf = conf.get_conf('CacheConfig.ini', 'Cache Expiration')
cache_state = int(cache_state_conf['default_timeout_interval'])

def initiate_cleaner():
    if cache_state == 0: return True
    table_names = ast.literal_eval(tables)

    while(True):
        for table in table_names:
            cleaning_time = datetime.datetime.now()
            logger.info("Cleaning time: {0}".format(cleaning_time))
            query = "DELETE FROM {0} WHERE expirydatetime <= '{1}'".format(table,cleaning_time)
            try:
                result = CC.delete_data(query)
                logger.info("{0} records deleted from {1}.".format(result,table))
                logger.info("Cleaning done!!!")
            except Exception, err:
                logger.error("Error cleaning tables!")
                logger.error(err)
        time.sleep(cache_cleaning_interval)


if __name__ == "__main__":
    initiate_cleaner()