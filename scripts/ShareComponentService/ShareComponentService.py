__author__ = 'Marlon Abeykoon'
__version__ = '1.0.1.1'

import json
import InternalSharing as intshare
import modules.CommonMessageGenerator as cmg

class ShareComponent():

    def __init__(self, method, **kwargs):
        self.method = method
        self.data = kwargs

    def share_component(self):

        if self.method == 'component_internal':
            share_obj = intshare.InternalSharing(self.data['security_level_auth'], self.data['comp_type'],# self.data['share_data'],
                                            self.data['UserID'], self.data['Domain'])
            share_result = share_obj.do_share(self.data['share_data'])
            if not json.loads(share_result)['Is_Success']:
                return share_result
            unshare_result = share_obj.undo_share(self.data['unshare_data'])
            if not json.loads(unshare_result)['Is_Success']:
                return unshare_result

            return cmg.format_response(True, {'successful_shares':share_obj.authorized_shares, 'unsuccessful_shares':share_obj.unauthorized_shares},
                                       'Components sharing process successful')