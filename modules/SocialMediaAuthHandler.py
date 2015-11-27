__author__ = 'Administrator'
import facebook
import facebookads


def set_token(token):
    auth = facebook.GraphAPI(token)
    return auth