__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import hashlib
import collections
import datetime

def get_id(params_obj,user_id=None):
    params_dict = dict(zip(params_obj.keys(), params_obj.values()))
    params_dict.pop("SecurityToken", None)
    if user_id is not None:
        params_dict['user_id'] = user_id
    sorted_params_dict = collections.OrderedDict(sorted(params_dict.items()))
    sorted_params_str = ', '.join(['{}_{}'.format(k,v) for k,v in sorted_params_dict.iteritems()])
    byte_seq = sorted_params_str.encode('utf-8')
    hash_object = hashlib.md5(byte_seq)
    hex_dig = hash_object.hexdigest()
    return hex_dig

def unix_time_millis_id(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)