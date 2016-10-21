__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import PackageProcessor as pp

class PackageHandler():

    def __init__(self, packages, tenant):
        self.packages = packages
        self.tenant = tenant

    def activate_packages(self):
        for package in self.packages:
            if not package['is_new']:
                pp.PackageProcessor(package['package_name'], package['package_attribute'], package['package_value'],package['package_price'], package['is_default'],self.tenant,package['package_id']).activate_packages()
            else:
                pp.PackageProcessor(package['package_name'], package['package_attribute'], package['package_value'],package['package_price'], package['is_default'],self.tenant).set_packages()




