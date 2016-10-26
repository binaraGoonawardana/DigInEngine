__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.1'
#code added by Thivatharan Jeganathan
import PackageProcessor as pp

class PackageHandler():

    def __init__(self, packages, tenant):
        self.packages = packages
        self.tenant = tenant

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

