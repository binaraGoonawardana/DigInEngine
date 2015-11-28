__author__ = 'Administrator'
import facebook
import facebookads


def set_token(token):
    try:
        auth = facebook.GraphAPI(token)
    except:
        "Error Occurred when getting data from Facebook"
        raise
    return auth