__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.1'

import UsageCalculatorJob as ucj
import configs.ConfigHandler as conf

scheduled_time = conf.get_conf('JobSchedules.ini','UsageCalculatorJob')

class DigInScheduler():

    def __init__(self, job_name, command):
        self.job_name = job_name
        self.command = command
        self.next_run = self._get_next_scheduled_time()

    def _get_next_scheduled_time(self):
        scheduled_time = conf.get_conf('JobSchedules.ini', self.job_name)
        return int(scheduled_time['job_run_frequency'])

    def start_job(self):
        if self.job_name == 'UsageCalculatorJob':
            ucj.UsageCalculatorJob(self.next_run, self.command).initiate_usage_scheduler()
