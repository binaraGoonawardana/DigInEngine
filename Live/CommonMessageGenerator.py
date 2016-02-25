__author__ = 'Marlon Abeykoon'

import json

exception = None


def format_response(is_success, result, custom_message, exception=None):
    if exception:
        globals()['exception'] = {
            'Message': exception.message,
            'Stack_Trace': exception.stack
        }

    result = {'Exception': globals()['exception'],
              'Custom_Message': custom_message,
              'Is_Success': is_success,
              'Result': result
              }

    return json.dumps(result)
