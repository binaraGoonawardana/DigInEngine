__author__ = 'Administrator'
import facebook


def set_token(token):
    auth = facebook.GraphAPI(token)
    return auth