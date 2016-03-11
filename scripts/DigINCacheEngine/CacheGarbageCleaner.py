__author__ = 'Marlon Abeykoon'
import datetime
import time
import sys
import logging
sys.path.append("...")
import scripts.DigINCacheEngine.CacheController as CC
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('CacheGarbageCleaner.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')
datasource_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
cache_cleaning_interval = float(datasource_settings['cache_cleaning_interval'])


def initiate_cleaner():

    table_names = ['cache_aggregation']

    while(True):
        for table in table_names:
            cleaning_time = datetime.datetime.now()
            logger.info("Cleaning time: {0}".format(cleaning_time))
            query = "DELETE FROM cache_aggregation WHERE expirydatetime <= '{0}'".format(cleaning_time)
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