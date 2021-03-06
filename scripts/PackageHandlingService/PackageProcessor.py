__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.4'
#code added by Thivatharan Jeganathan

import datetime
import modules.CommonMessageGenerator as cmg
import sys
import scripts.DigINCacheEngine.CacheController as db
import calendar
import configs.ConfigHandler as conf

free_package = conf.get_conf('DefaultConfigurations.ini', 'Package Settings')['Free']
default_1 = conf.get_conf('DefaultConfigurations.ini', 'Package Settings')['Personal Space']
default_2 = conf.get_conf('DefaultConfigurations.ini', 'Package Settings')['We Are A Mini Team']
default_3 = conf.get_conf('DefaultConfigurations.ini', 'Package Settings')['We Are the World']

class PackageProcessor():

    def __init__(self, package_name, package_attribute, package_value, package_price, is_default, tenant, package_id=None, start_date =None, end_date=None):
        self.package_id = self._unix_time_millis(datetime.datetime.now()) if not package_id else package_id
        self.package_name = package_name
        self.package_attribute = package_attribute
        self.package_value = package_value
        self.package_price = package_price
        self.is_default = is_default
        self.tenant = tenant
        self.start_date = start_date
        self.end_date = end_date

    def _unix_time_millis(self,dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return int((dt - epoch).total_seconds() * 1000.0)

    def get_package_summary(self):
        query = "SELECT " \
                "a.package_attribute, " \
                "SUM(a.package_value), " \
                "SUM(a.package_price) " \
                "FROM digin_packagedetails a " \
                "INNER JOIN digin_tenant_package_details b " \
                "ON a.package_id = b.package_id " \
                "WHERE b.tenant_id = '{0}' AND b.package_status = 'current_package' " \
                "GROUP BY a.package_attribute".format(self.tenant)

        try:
            result = db.get_data(query)['rows']
            data_list = []
            for row in result:
                data = {'package_attribute': row[0],
                        'package_value_sum': row[1],
                        'package_price_sum': row[2]}
                data_list.append(data)
        except Exception, err:
            print err
            return cmg.format_response(False, err, "Error occurred while getting data", exception=sys.exc_info())
        return cmg.format_response(True, data_list, "Package summary retrieved successfully")

    def get_package_detail(self):
        query = "SELECT " \
                "a.package_id, " \
                "a.package_name, " \
                "a.package_attribute, " \
                "SUM(a.package_value), " \
                "SUM(a.package_price), " \
                "b.expiry_datetime, " \
                "TIMESTAMPDIFF(DAY, CURRENT_TIMESTAMP, expiry_datetime) as remaining_days, " \
                "CURRENT_TIMESTAMP > b.expiry_datetime, " \
                "b.user_status " \
                "FROM digin_packagedetails a " \
                "INNER JOIN digin_tenant_package_details b " \
                "ON a.package_id = b.package_id " \
                "WHERE b.tenant_id = '{0}' AND b.package_status = 'current_package' " \
                "GROUP BY a.package_id, a.package_name, a.package_attribute, b.expiry_datetime, remaining_days".format(self.tenant)

        try:
            result = db.get_data(query)['rows']
            data_list = []
            for row in result:
                data = {'package_id': row[0],
                        'package_name': row[1],
                        'package_attribute': row[2],
                        'package_value_sum': row[3],
                        'package_price_sum': row[4],
                        'expiry_datetime': row[5],
                        'remaining_days': row[6],
                        'is_expired': bool(row[7]),
                        'user_status': row[8]}
                data_list.append(data)
        except Exception, err:
            print err
            return cmg.format_response(False, err, "Error occurred while getting data", exception=sys.exc_info())
        return cmg.format_response(True, data_list, "Package details retrieved successfully")

    def get_ledger(self):
        # query = "SELECT " \
        #         "a.package_id, " \
        #         "a.package_name, " \
        #         "a.package_attribute, " \
        #         "a.package_value, " \
        #         "a.package_price, " \
        #         "b.expiry_datetime, " \
        #         "TIMESTAMPDIFF(DAY, CURRENT_TIMESTAMP, expiry_datetime) as remaining_days, " \
        #         "b.package_status, " \
        #         "b.created_datetime " \
        #         "FROM digin_packagedetails a " \
        #         "INNER JOIN digin_tenant_package_details b " \
        #         "ON a.package_id = b.package_id " \
        #         "WHERE b.tenant_id = '{0}' " \
        #         "AND b.created_datetime >= TIMESTAMP('{1}') AND  b.created_datetime <= TIMESTAMP('{2}') " \
        #         "GROUP BY a.package_id, a.package_name, a.package_attribute, b.expiry_datetime, remaining_days " \
        #         "ORDER BY b.created_datetime ".format(self.tenant, self.start_date, self.end_date)

        query = "SELECT " \
                "package_id, " \
                "expiry_datetime, " \
                "TIMESTAMPDIFF(DAY, CURRENT_TIMESTAMP, expiry_datetime) as remaining_days, " \
                "package_status, " \
                "created_datetime " \
                "FROM digin_tenant_package_details  " \
                "WHERE tenant_id = '{0}' " \
                "AND created_datetime >= TIMESTAMP('{1}') AND  created_datetime <= TIMESTAMP('{2}') " \
                "ORDER BY created_datetime ".format(self.tenant, self.start_date, self.end_date)


        try:
            result = db.get_data(query)['rows']
            data_list = []
            for row in result:
                data = {'package_id': row[0],
                        'package_Details': PackageProcessor(package_name=None, package_attribute=None, package_value=None, package_price=None,is_default=False, tenant =self.tenant,package_id=int(row[0])).get_package_attributes(),
                        'expiry_datetime': row[1],
                        'remaining_days': row[2],
                        'package_status': row[3],
                        'created_datetime': row[4]}
                data_list.append(data)
        except Exception, err:
            print err
            return cmg.format_response(False, err, "Error occurred while getting data", exception=sys.exc_info())
        return cmg.format_response(True, data_list, "Package details retrieved successfully")

    def get_package_attributes(self):

        package= self.package_id

        query = "SELECT " \
                "package_id, " \
                "package_name, " \
                "package_attribute, " \
                "package_value, " \
                "package_price " \
                "FROM digin_packagedetails " \
                "WHERE package_id = {0} ".format(self.package_id)
        try:
            result = db.get_data(query)['rows']
            data_list = []
            for row in result:
                data = {
                        'package_name': row[1],
                        'package_attribute': row[2],
                        'package_value': row[3],
                        'package_price': row[4]}
                data_list.append(data)
        except Exception, err:
            print err
            return "Error occurred while getting data"
        return data_list


    def set_packages(self):
        time_now = datetime.datetime.now()
        _, num_days = calendar.monthrange(time_now.year, time_now.month)
        free_package = conf.get_conf('DefaultConfigurations.ini', 'Package Settings')['Free']
        if self.package_id == int(free_package):
            last_day = time_now + datetime.timedelta(days=30)
        else:
            last_day = datetime.datetime(time_now.year, time_now.month, num_days, 23, 59, 59)

        tenant_package_mapping = [{'tenant_id': self.tenant,
                                   'package_id': self.package_id,
                                   'created_datetime': time_now,
                                   'modified_datetime': time_now,
                                   'expiry_datetime': last_day,
                                   'package_status':'current_package'}]
        try:
            db.insert_data(tenant_package_mapping, 'digin_tenant_package_details')
        except Exception, err:
            print "Error inserting to cacheDB!"
            return cmg.format_response(False, err, "Error occurred while inserting.. \n" + str(err),
                                          exception=sys.exc_info())

        if not self.is_default:
            package_data = [{'package_id': self.package_id,
                             'package_name': self.package_name,
                             'package_attribute': self.package_attribute,
                             'package_value': self.package_value,
                             'package_price': self.package_price,
                             'is_default': False}]
            try:
                db.insert_data(package_data, 'digin_packagedetails')
            except Exception, err:
                print "Error inserting to DB!"
                result = cmg.format_response(False, err,
                                             "Error occurred while inserting additional_packages.. \n" + str(err),
                                             exception=sys.exc_info())
                return result
        return cmg.format_response(True, 0, "Package updated successfully")

    def deactivate_packages(self):
        ""
        if self.is_default:
            try:
                db.update_data('digin_tenant_package_details'," WHERE tenant_id = '{0}' AND package_id IN ({1},{2},{3},{4})".format(self.tenant,int(free_package),int(default_1),int(default_2),int(default_3)),
                               package_status = 'deactivated')
            except Exception, err:
                print "Error inserting to DB!"
                return cmg.format_response(False, err, "Error occurred while deactivate_packages.. \n" + str(err),
                                              exception=sys.exc_info())

        else:
            try:
                db.update_data('digin_tenant_package_details',
                               " WHERE package_id = '{0}' AND tenant_id = '{1}'".format(self.package_id, self.tenant),
                               package_status='deactivated')
            except Exception, err:
                print "Error inserting to DB!"
                return cmg.format_response(False, err,
                                           "Error occurred while deactivate additional_packages.. \n" + str(err),
                                           exception=sys.exc_info())
