__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.1'


from threading import Timer
import datetime
import logging
import sys
sys.path.append("...")
import modules.BigQueryHandler as bq
import scripts.DigInRatingEngine.DigInRatingEngine as dre
import scripts.DigINCacheEngine.CacheController as db
import configs.ConfigHandler as conf

path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/UsageCalculatorJob.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')

class UsageCalculatorJob():

    def __init__(self, next_run=None, command=None):
        self.command = command #TODO start, stop, restart using console commands
        self.next_run = next_run
        self.run_count = 0

    def initiate_usage_scheduler(self):

            usage_scheduler_run_time = datetime.datetime.now()
            if self.run_count != 0:
                print 'usage_scheduler started at: ' + str(usage_scheduler_run_time)
                logger.info('usage_scheduler started at: ' + str(usage_scheduler_run_time))
                datasets = bq.get_datasets()
                for dataset in datasets:
                    try:
                        storage_query = "SELECT SUM(size_bytes) as storage_bq FROM [{0}.__TABLES__]".format(
                            dataset['datasetReference']['datasetId'])
                        storage_bq = bq.execute_query(storage_query, user_id=0, tenant='DigInEngine')[0]['storage_bq']
                        user_id = db.get_data(
                            "SELECT user_id FROM digin_user_settings WHERE REPLACE(REPLACE(email, '.', '_'), '@', '_') = '{0}' limit 1".format(
                                dataset['datasetReference']['datasetId']))['rows']
                        if user_id == ():
                            print 'No user_Settings found for user: ' + dataset['datasetReference']['datasetId']
                            logger.info('No user_Settings found for user: ' + dataset['datasetReference']['datasetId'])
                        else:
                            usages = {'storage_bq': 0 if storage_bq is None else storage_bq}
                            obj = dre.RatingEngine(user_id[0][0], 'undefined', **usages)
                            obj.set_usage()
                            print 'Storage calculated for user ' + str(user_id[0][0])
                            logger.info('Storage calculated for user ' + str(user_id[0][0]))
                    except Exception, err:
                        print err
            self.run_count += 1
            t = Timer(self.next_run, self.initiate_usage_scheduler)
            t.start()




