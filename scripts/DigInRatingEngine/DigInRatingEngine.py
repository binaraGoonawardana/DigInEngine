__author__ = 'Marlon Abeykoon'

import modules.CommonMessageGenerator as cmg

def get_component_count_temp(user_id,tenant):
    print user_id
    obj = {
        'tenant':tenant,
        'dashboard_count' : 5,
        'report_count': 2,
        'bandwidth': 4,
        'storage': 7,
        'is_blocked': False
    }
    return cmg.format_response('True',obj,"Usage data retrieved")