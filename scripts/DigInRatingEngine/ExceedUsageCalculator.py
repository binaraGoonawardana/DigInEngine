__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.1'

from scripts.PackageHandlingService import PackageProcessor as pp
import scripts.DigINCacheEngine.CacheController as db
import json
import modules.CommonMessageGenerator as cmg
import sys

class ExceedUsageCalculator():

    def __init__(self, tenant,attribute):
        self.tenant = tenant
        self.attribute = attribute

    def calculation(self):
        usage_limit = pp.PackageProcessor(package_name=None, package_attribute=None, package_value=None, package_price=None,
                                is_default=False, tenant =self.tenant).get_package_summary()
        data_dic = {}
        usage_limit = json.loads(usage_limit)
        for data in usage_limit["Result"]:
            data_dic[data['package_attribute']] = data['package_value_sum'] < ExceedUsageCalculator(tenant=self.tenant,attribute=data['package_attribute']).get_usage()

        return data_dic

    def get_usage(self):
        if self.attribute == "data":
            try:
                summary = db.get_data(" SELECT sum(value) FROM digin_usage_summary "
                                      " WHERE tenant = '{0}' AND parameter = 'download_bq' OR 'upload_size_bq' ".format(self.tenant))['rows']
                return summary[0]
            except Exception, err:
                print "Error inserting to DB!"
                return cmg.format_response(False, err, "Error occurred while getting packages details .. \n" + str(err),
                                              exception=sys.exc_info())

        elif self.attribute == "storage":
            try:
                summary = db.get_data(" SELECT sum(value) FROM digin_usage_summary "
                                      " WHERE tenant = '{0}' AND parameter = 'storage_bq' ".format(self.tenant))['rows']
                return summary[0]
            except Exception, err:
                print "Error inserting to DB!"
                return cmg.format_response(False, err, "Error occurred while getting packages details .. \n" + str(err),
                                              exception=sys.exc_info())

        elif self.attribute == "users":
            try:
                summary = db.get_data(" SELECT sum(value) FROM digin_usage_summary "
                                      " WHERE tenant = '{0}' AND parameter = 'users' ".format(self.tenant))['rows']
                return summary[0]
            except Exception, err:
                print "Error inserting to DB!"
                return cmg.format_response(False, err, "Error occurred while getting packages details .. \n" + str(err),
                                              exception=sys.exc_info())