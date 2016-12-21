__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.1'
#code added by Thivatharan Jeganathan
import PackageProcessor as pp
import modules.CommonMessageGenerator as comm
import json

class PackageHandler():

    def __init__(self, packages, tenant, start_date=None, end_date=None):
        self.packages = packages
        self.tenant = tenant
        self.start_date = start_date
        self.end_date = end_date

    def activate_packages(self):
        for package in self.packages:
            if not package['is_new']:
                if package['is_default']:
                    result = pp.PackageProcessor(package['package_name'], package['package_attribute'], package['package_value'],package['package_price'], package['is_default'],self.tenant,package['package_id']).deactivate_packages()
                    print "previous package deactivated"
                    result = pp.PackageProcessor(package['package_name'], package['package_attribute'],package['package_value'], package['package_price'], package['is_default'],self.tenant, package['package_id']).set_packages()
                else:
                    result = pp.PackageProcessor(package['package_name'], package['package_attribute'],
                                                 package['package_value'], package['package_price'],
                                                 package['is_default'], self.tenant,
                                                 package['package_id']).deactivate_packages()
            else:
                result = pp.PackageProcessor(package['package_name'], package['package_attribute'], package['package_value'],package['package_price'], package['is_default'],self.tenant,package['package_id']).set_packages()
        return result

    def get_packages(self, params):
        if params.get_type == 'summary':
            result = pp.PackageProcessor(package_name=None, package_attribute=None, package_value=None, package_price=None,
                                is_default=False, tenant =self.tenant).get_package_summary()
            return result
        elif params.get_type == 'detail':
            result = pp.PackageProcessor(package_name=None, package_attribute=None, package_value=None,
                                         package_price=None,
                                         is_default=False, tenant=self.tenant).get_package_detail()
            return result

        elif params.get_type == 'ledger':
            result = pp.PackageProcessor(package_name=None, package_attribute=None, package_value=None,
                                         package_price=None,
                                         is_default=False, tenant=self.tenant, start_date=params.start_date,
                                         end_date=params.end_date).get_ledger()
            return result


    def get_initial_package_details(self):

        result = pp.PackageProcessor(package_name=None, package_attribute=None, package_value=None,
                                     package_price=None,
                                     is_default=False, tenant=self.tenant).get_package_detail()
        a=json.loads(result)

        packages = a['Result']

        for data in packages:
            if data['package_id'] == '1005':
                return result

        return comm.format_response(False, 'No record Found', 'No record Found', exception=None)