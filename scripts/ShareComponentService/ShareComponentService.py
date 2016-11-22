__author__ = 'Marlon Abeykoon'
__version__ = '1.0.1.0'

import InternalSharing as intshare

class ShareComponent():

    def __init__(self, method, **kwargs):
        self.method = method
        self.data = kwargs

    def share_component(self):
        if self.method == 'component_internal':
            return intshare.InternalSharing(self.data['security_level_auth'], self.data['comp_type'], self.data['share_data'],
                                            self.data['UserID'], self.data['Domain']).do_share()