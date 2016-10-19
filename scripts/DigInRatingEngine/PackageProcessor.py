__author__ = 'Jeganathan Thivatharan'
__version__ = '1.0.0.0'

import datetime
import modules.CommonMessageGenerator as cmg
import sys
import scripts.DigINCacheEngine.CacheController as db



epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

def active_package(params,userId,tenant):

    if params['function'].lower() == 'active':

        default_package = params['default_package']
        summary = db.get_data("SELECT package_id "
                              "FROM digin_packagedetails "
                              "WHERE package_name =  '{0}'".format(default_package))['rows']

        for default_id in summary:
            a= default_id[0]
            data = [{'tenant_id':tenant,
                    'package_id':default_id[0],
                    'created_date':datetime.datetime.now(),
                    'modified_date':datetime.datetime.now(),
                    'package_status':'Active'}]
            try:
                result = db.insert_data(data, 'digin_tenant_packagedetails')
            except Exception, err:

                print "Error inserting to cacheDB!"
                result = cmg.format_response(False, err, "Error occurred while inserting.. \n" + str(err),
                                              exception=sys.exc_info())
                return result

        additional_packages = params['additional_packages']
        if additional_packages!= None:

            for package in additional_packages:
                package_id = int(unix_time_millis(datetime.datetime.now()))
                package_data = [{'package_id': package_id,
                                'package_name':'UserDefine',
                                'package_attribute':package['attribute'],
                                'package_value':str(package['value']),
                                'package_price':str(package['price']),
                                'is_default':False}]
                try:
                    result = db.insert_data(package_data, 'digin_packagedetails')
                except Exception, err:
                    print "Error inserting to DB!"
                    result = cmg.format_response(False, err, "Error occurred while inserting additional_packages.. \n" + str(err),
                                                  exception=sys.exc_info())
                    return result

                tenant_data = [{'tenant_id': tenant,
                         'package_id': package_id,
                         'created_date': datetime.datetime.now(),
                         'modified_date': datetime.datetime.now(),
                         'package_status': 'Active'}]
                try:
                    result = db.insert_data(tenant_data, 'digin_tenant_packagedetails')
                except Exception, err:
                    print "Error inserting to DB!"
                    result = cmg.format_response(False, err, "Error occurred while inserting additional_packages.. \n" + str(err),
                                                  exception=sys.exc_info())
                    return result

        return cmg.format_response(True, result, " Package activated successfully")

    elif params['function'] =='update':
        package_id = params['package_id']
        package_status = params['package_status']

        update_data = [{'package_id': package_id,
                         'modified_date': datetime.datetime.now(),
                         'package_status': package_status}]
        try:
            result = db.insert_data(update_data, 'digin_tenant_packagedetails')
        except Exception, err:
            print "Error inserting to DB!"
            result = cmg.format_response(False, err, "Error occurred while inserting additional_packages.. \n" + str(err),
                                          exception=sys.exc_info())
            return result

    return cmg.format_response(True, result, " Package activated successfully")


def get_tenant_package(tenant):

    summary = []
    try:
        summary = db.get_data("SELECT package_name,package_attribute,package_value,package_price "
                                "FROM digin_packagedetails "
                                "WHERE package_id in (select package_id from digin_tenant_packagedetails where tenant_id = '{0}')".format(tenant))['rows']
    except Exception, err:
        print "Error inserting to DB!"
        result = cmg.format_response(False, err, "Error occurred while inserting additional_packages.. \n" + str(err),
                                     exception=sys.exc_info())
    data = []
    for packages in summary:
        row = {'package':packages[0],
               'attribute':packages[1],
                'value':packages[2],
                'price':packages[3]}
        data.append(row)

    return cmg.format_response(True, summary, "Package Details")
