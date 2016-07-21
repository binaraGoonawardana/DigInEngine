__author__ = 'Marlon Abeykoon'

import json
import traceback
from datetime import datetime, date
import decimal
import numpy as np

class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        if isinstance(obj, datetime) or isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, np.int64):
            return np.asscalar(np.int64(obj))
        return super(ExtendedJSONEncoder, self).default(obj)


exception = None


def format_response(is_success, result, custom_message, exception=None):
    if exception:
        type_, value_, traceback_ = exception
        tb = traceback.format_exc(traceback_)
        exception={
            'Message': str(value_),
            'Type': str(type_),
            'Traceback': tb
        }

    result = {'Exception': exception,
              'Custom_Message': custom_message,
              'Is_Success': is_success,
              'Result': result
              }

    return json.dumps(result, cls=ExtendedJSONEncoder)
