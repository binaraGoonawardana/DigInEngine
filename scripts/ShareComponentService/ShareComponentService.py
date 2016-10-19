__author__ = 'Marlon Abeykoon'
__version__ = '1.0.1.0'

import InternalSharing as intshare

class ShareComponent():

    def __init__(self, method, **kwargs):
        self.method = method
        self.data = kwargs

    def share_component(self):
        if self.method == 'component_internal':
            intshare.InternalSharing(self.data['other_data'],self.data['Domain'],self.data['id']).do_share()