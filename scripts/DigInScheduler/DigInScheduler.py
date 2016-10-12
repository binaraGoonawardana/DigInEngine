__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import UsageCalculatorJob as ucj

class DigInScheduler():

    def __init__(self, job_name, command):
        self.job_name = job_name
        self.command = command

    def start_job(self):
        if self.job_name == 'UsageCalculatorJob':
            ucj.UsageCalculatorJob(self.command).initiate_usage_scheduler()
