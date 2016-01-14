__author__ = 'Marlon Abeykoon'

import FacebookAnalytics as FB
import TwitterAnalytics as Tw
import LiveStreamInitializer as lsi
import SocialMediaLiveFeeds as smlf
import sys
sys.path.append("...")
import modules.SocialMediaAuthHandler as SMAuth
import modules.sentimentAnalysis as sa
import json
import web
import ast
import logging

urls = (
    '/pageoverview(.*)', 'FBOverview',
    '/demographicsinfo(.*)', 'FBPageUserLocations',
    '/fbpostswithsummary(.*)', 'FBPostsWithSummary',
    '/promtionalinfo(.*)', 'FBPromotionalInfo',
    '/twitteraccinfo(.*)', 'TwitterAccInfo',
    '/hashtag(.*)', 'BuildWordCloud',
    '/buildwordcloudrt(.*)', 'BuildWordCloudRT',
    '/streamingtweets(.*)', 'StreamingTweets',
    '/sentimentanalysis(.*)', 'SentimentAnalysis',
    '/test(.*)', 'Test'
)

app = web.application(urls, globals())
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('SocialMediaService.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  SocialMediaService  ---------------------------------------------')
logger.info('Starting log')


# http://localhost:8080/pageoverview?metric_names=['page_views']&token=CAACEdEosecBAMs8o7vZCgwsufVOQcLynVtFzCq6Ii1LwMyMRFgcV5xFPzUWGMKfJBJZBGb33yDciESrnThNY4mAV2fn14cGEjSUZAIvx0jMt4g6M3lEO8arfNPZBDISA49vO9F7LsKQwyePkWJBSN8NuMvaIWGzTfOrkpQzItLTlSSweUX8LOZB4TQRi8p8ZD&since=1447509660&until=1448028060
class FBOverview(web.storage):
    def GET(self, r):

        token = web.input().token
        metric_names = None
        try:
            metric_names = ast.literal_eval(web.input().metric_names)
        except AttributeError:
            pass
        since = None
        until = None
        try:
            since = web.input().since
            until = web.input().until
        except AttributeError:
            pass
        logger.info('Requested received: %s' % web.input().values())
        # data = json.dumps(fb.insight_metric(token, metric_name, since, until))
        data = json.dumps(FB.get_overview(token, metric_names, since, until))
        print data
        return data


class FBPageUserLocations(web.storage):
    def GET(self, r):
        token = web.input().token
        logger.info('Requested received: %s' % web.input().values())
        data = json.dumps(FB.get_page_fans_city(token))
        return data

#http://localhost:8080/fbpostswithsummary?token=CAACEdEose0cBAJ6yiM46CxqzY3UyaTiSO2hwj451gctPwPULXdTQo8hmlufxjcKjTKySKQchoiRvBmodWivQ97tqTOmsfcZAt4b0ROwZCbFsrZAb3ED0sq3e4GbxL6OdjyAE26H9qBYs05msMm1uPdKRw2lLLfurThOKgTxdJwvTDgZBjMSxtJu6QYxLMcrgwgrzpJxZCQZDZD&limit=20&since=2015-07-01&until=2015-11-25
class FBPostsWithSummary(web.storage):
    def GET(self, r):
        token = web.input().token
        limit = ''
        since = ''
        until = ''
        page = 'me'
        try:
            page = str(web.input().page)
            limit = web.input().limit
            since = web.input().since
            until = web.input().until
        except AttributeError:
            pass
        logger.info('Request received: %s' % web.input().values())
        data = json.dumps(FB.get_page_posts(token, limit, since, until, page=page))
        return data


class FBPromotionalInfo(web.storage):
    def GET(self, r):
        token = web.input().token
        promotional_name = web.input().metric_name
        data = json.dumps(FB.get_promotional_info(token, promotional_name))
        print data

#http://localhost:8080/twitteraccinfo?ids=[%271123728482,5539932%27]&tokens={%27consumer_key%27:%27xHl7DEIJjH8pNM2kn8Q9EddGy%27,%27consumer_secret%27:%27eVxjTk7d4Z41VQ2Kt7kcOF6aFjTQqqiWIKgM8xhqkMYoE8Pxmq%27,%27access_token%27:%2779675949-r2z1UIBa5eeiIQBO6e4PSLytCMpfPUHC2lNoI7o2%27,%27access_token_secret%27:%27dBH5sLkief3oz7sftVwP30at1fij9dFm4hL02tpCUFxbj%27}
class TwitterAccInfo(web.storage):
    def GET(self, r):
        tokens = ast.literal_eval(web.input().tokens)
        id_list = ast.literal_eval(web.input().ids)
        api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
        data = json.dumps(Tw.get_account_summary(api, id_list))
        return data

#http://localhost:8080/hashtag?hashtag=%27%23get%27&tokens={%27consumer_key%27:%27xHl7DIJjH8pNM2kn8Q9EddGy%27,%27consumer_secret%27:%27xHl7DEIJjH8NM2kn8Q9EddGy%27,%27access_token%27:%2779675949-r2z1UIBa5eeiIQBO6e4PSL9ytCMpfPUHC2lNoI7o2%27,%27access_token_secret%27:%27dBH5sLkief3oz7sftVP30at1fij9dFm4hL02tpCUFxbj%27}
class BuildWordCloud(web.storage):
    def GET(self, r):
        tokens = ast.literal_eval(web.input().tokens)
        hash_tag = web.input().hashtag
        api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
        data = Tw.hashtag_search(api, hash_tag)
        return data

#http://localhost:8080/buildwordcloudrt?tokens=%27rr%27&hashtag=earthquake&unique_id=eq
class BuildWordCloudRT(web.storage):
    def GET(self, r):
        tokens = ast.literal_eval(web.input().tokens)
        hash_tag = str(web.input().hashtag)
        unique_id = str(web.input().unique_id)

        lsi.initialize_stream(hash_tag, unique_id, tokens) # if already exits do something
        data = smlf.process_social_media_data(unique_id, hash_tag)
        return data

#http://localhost:8080/sentimentanalysis?tokens=%27rr%27&hashtag=earthquake&unique_id=eq
class SentimentAnalysis(web.storage):
    def GET(self, r):
        tokens = ast.literal_eval(web.input().tokens)
        source = str(web.input().source)
        limit = ''
        since = ''
        until = ''
        page = 'me'
        try:
            page = str(web.input().page)
            unique_id = str(web.input().unique_id)
            hash_tag = str(web.input().hash_tag)
            limit = web.input().limit
            since = web.input().since
            until = web.input().until
        except AttributeError:
            pass
        data = 'Incorrect datasource name provided!'
        if source == 'twitter':
            lsi.initialize_stream(hash_tag, unique_id, tokens) # if already exits do something
            data = smlf.process_social_media_data(unique_id, hash_tag)
        elif source == 'facebook':
            data = FB.get_page_posts_comments(tokens, limit, since, until, page)
            full_comment_str = ''
            full_comment = []
            for post in data:
                for comments in post['comments']:
                   comment = comments['message']
                   full_comment.append(comment)
            data = sa.sentiment(full_comment_str.join(full_comment))
        return data

class Test(web.storage):
    def GET(self, r):
        token = web.input().token
        limit = ''
        since = ''
        until = ''
        page = 'me'
        try:
            page = str(web.input().page)
            limit = web.input().limit
            since = web.input().since
            until = web.input().until
        except AttributeError:
            pass
        logger.info('Request received: %s' % web.input().values())
        data = FB.get_page_posts_comments(token, limit, since, until, page  )
        return json.dumps(data)

class StreamingTweets(web.storage):
    def GET(self, r):
        data = Tw.get_streaming_tweets('123id', 'obama', 10)
        return data

if __name__ == "__main__":
    app.run()
