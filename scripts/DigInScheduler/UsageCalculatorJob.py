__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import threading
import datetime
import time
import sys
sys.path.append("...")
import modules.BigQueryHandler as bq
import scripts.DigInRatingEngine.DigInRatingEngine as dre
import scripts.DigINCacheEngine.CacheController as db
import configs.ConfigHandler as conf

usage_scheduled_period = conf.get_conf('DefaultConfigurations.ini', 'System Settings')['usage_scheduled_interval']

class UsageCalculatorJob():

    def __init__(self, command, dataset=None):
        self.command = command #TODO start, stop, restart using console commands
        self.dataset = dataset

    def _calculate_usage(self):

            try:
                storage_query = "SELECT SUM(size_bytes) as storage_bq FROM [{0}.__TABLES__]".format(
                    self.dataset['datasetReference']['datasetId'])
                storage_bq = bq.execute_query(storage_query, user_id=0, tenant='DigInEngine')[0]['storage_bq']
                user_id = db.get_data(
                    "SELECT user_id FROM digin_user_settings WHERE REPLACE(REPLACE(email, '.', '_'), '@', '_') = '{0}' limit 1".format(
                        self.dataset['datasetReference']['datasetId']))['rows']
                if user_id == ():
                    print 'No user_Settings found for user: ' + self.dataset['datasetReference']['datasetId']
                else:
                    usages = {'storage_bq': 0 if storage_bq is None else storage_bq}
                    obj = dre.RatingEngine(user_id[0][0], 'undefined', **usages)
                    obj.set_usage()
                    print 'Storage calculated for user ' + str(user_id[0][0])
            except Exception, err:
                print err

    def initiate_usage_scheduler(self):
        while True:
            usage_scheduler_run_time = datetime.datetime.now()
            print 'usage_scheduler started at: '+  str(usage_scheduler_run_time)
            print 'usage_scheduler will run every ' + str(usage_scheduled_period) + ' sec'
            datasets = bq.get_datasets()
            print datasets
            for dataset in datasets:
                self.dataset = dataset
                p = threading.Thread(target=self._calculate_usage, args=())
                p.start()

            time.sleep(int(usage_scheduled_period))