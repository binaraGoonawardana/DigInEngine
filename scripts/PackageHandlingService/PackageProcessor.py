__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import datetime
import modules.CommonMessageGenerator as cmg
import sys
import scripts.DigINCacheEngine.CacheController as db

class PackageProcessor():

    def __init__(self, package_name, package_attribute, package_value, package_price, is_default, tenant, package_id=None):
        self.package_id = self._unix_time_millis(datetime.datetime.now()) if not package_id else package_id
        self.package_name = package_name
        self.package_attribute = package_attribute
        self.package_value = package_value
        self.package_price = package_price
        self.is_default = is_default
        self.tenant = tenant

    def _unix_time_millis(self,dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds() * 1000.0)

    def _get_package_summary(self):
        ""

    def _get_package_detail(self):
        ""
    def set_packages(self):

        time_now = datetime.datetime.now()
        tenant_package_mapping = [{'tenant_id': self.tenant,
                                   'package_id': self.package_id,
                                   'created_datetime': time_now,
                                   'modified_datetime': time_now,
                                   'expiry_datetime': time_now + datetime.timedelta(days=30)}]
        try:
            db.insert_data(tenant_package_mapping, 'digin_tenant_package_details')
        except Exception, err:
            print "Error inserting to cacheDB!"
            return cmg.format_response(False, err, "Error occurred while inserting.. \n" + str(err),
                                          exception=sys.exc_info())

        if not self.is_default:
            package_data = [{'package_id': self.package_id,
                             'package_name': 'UserDefine',
                             'package_attribute': self.package_attribute,
                             'package_value': self.package_value,
                             'package_price': 0, # TODO calculate price
                             'is_default': False}]
            try:
                db.insert_data(package_data, 'digin_packagedetails')
            except Exception, err:
                print "Error inserting to DB!"
                result = cmg.format_response(False, err,
                                             "Error occurred while inserting additional_packages.. \n" + str(err),
                                             exception=sys.exc_info())
                return result

    def activate_packages(self):
        ""
        try:
            db.update_data('digin_tenant_package_details'," WHERE package_id = '{0}' AND tenant_id = '{1}'".format(self.package_id,self.tenant),
                           modified_datetime=datetime.datetime.now(),expiry_datetime=datetime.datetime.now()+ datetime.timedelta(days=30))
        except Exception, err:
            print "Error inserting to DB!"
            return cmg.format_response(False, err, "Error occurred while inserting additional_packages.. \n" + str(err),
                                          exception=sys.exc_info())


    def get_packages(self):
        ""


