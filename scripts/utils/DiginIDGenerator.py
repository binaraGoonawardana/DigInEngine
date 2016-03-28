__author__ = 'Marlon Abeykoon'

import hashlib
import collections

def get_id(params_obj,user_id):
    params_dict = dict(zip(params_obj.keys(), params_obj.values()))
    params_dict.pop("SecurityToken", None)
    params_dict['user_id'] = user_id
    sorted_params_dict = collections.OrderedDict(sorted(params_dict.items()))
    sorted_params_str = ', '.join(['{}_{}'.format(k,v) for k,v in sorted_params_dict.iteritems()])
    byte_seq = sorted_params_str.encode('utf-8')
    hash_object = hashlib.md5(byte_seq)
    hex_dig = hash_object.hexdigest()
    return hex_dig
