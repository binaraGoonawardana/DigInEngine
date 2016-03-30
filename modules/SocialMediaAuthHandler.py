__author__ = 'Administrator'
import facebook
import tweepy


def set_token(token):
    try:
        auth = facebook.GraphAPI(token)
    except Exception, err:
        print err
        "Error Occurred when getting data from Facebook"
        raise
    return auth

# def twitter_auth(consumer_key, consumer_secret, access_token, access_token_secret):
#     api = twitter.Api(consumer_key =  consumer_key,
#                       consumer_secret= consumer_secret,
#                       access_token_key= access_token,
#                       access_token_secret= access_token_secret)
#     try:
#         auth = api.VerifyCredentials()
#         print auth
#     except Exception, err:
#         print err
#         raise
#     return auth

def tweepy_auth(consumer_key, consumer_secret, access_token, access_token_secret):

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    api = tweepy.API(auth)
    auth.set_access_token(access_token, access_token_secret)
    return api