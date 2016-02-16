__author__ = 'Marlon Abeykoon'

import json
import traceback

exception = None


def format_response(is_success, result, custom_message, exception=None):
    if exception:
        type_, value_, traceback_ = exception
        tb = traceback.format_exc(traceback_)
        globals()['exception'] = {
            'Message': str(value_),
            'Type': str(type_),
            'Traceback': tb
        }

    result = {'Exception': globals()['exception'],
              'Custom_Message': custom_message,
              'Is_Success': is_success,
              'Result': result
              }

    return json.dumps(result)
