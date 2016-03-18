__author__ = 'Marlon Abeykoon'
__version__ = '1.2.1'

import FacebookAnalytics as FB
import TwitterAnalytics as Tw
import LiveStreamInitializer as lsi
import SocialMediaLiveFeeds as smlf
import sys
sys.path.append("...")
import modules.CommonMessageGenerator as cmg
import modules.SocialMediaAuthHandler as SMAuth
import modules.sentimentAnalysis as sa
import modules.bipartite as bp
import modules.wordcloud_ntstreaming as wc
import json
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
    '/buildwordcloudFB(.*)', 'BuildWordCloudFB',
    '/buildbipartite(.*)', 'BuildBiPartite',
    '/test(.*)', 'Test'
)

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
def fb_overview(params):

        token = params.token
        metric_names = None
        try:
            metric_names = ast.literal_eval(params.metric_names)
        except AttributeError:
            pass
        since = None
        until = None
        try:
            since = params.since
            until = params.until
        except AttributeError:
            pass
        logger.info('Requested received: %s' % params.values())
        # data = json.dumps(fb.insight_metric(token, metric_name, since, until))
        try:
            data_ = FB.get_overview(token, metric_names, since, until)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except:
            data = cmg.format_response(False,None,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data

def fb_page_user_locations(params):

        token = params.token
        logger.info('Requested received: %s' % params.values())
        try:
            data_ = FB.get_page_fans_city(token)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except:
            data = cmg.format_response(False,None,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data


def fb_posts_with_summary(params):

        token = params.token
        limit = ''
        since = ''
        until = ''
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        try:
            limit = params.limit
        except AttributeError:
            pass
        try:
            since = params.since
        except AttributeError:
            pass
        try:
            until = params.until
        except AttributeError:
            pass
        logger.info('Request received: %s' % params.values())
        try:
            data_ = FB.get_page_posts(token, limit, since, until, page=page)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except:
            data = cmg.format_response(False,None,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data

def fb_promotional_info(params):

        token = params.token
        promotional_name = params.metric_name
        try:
            data_ = FB.get_promotional_info(token, promotional_name)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except:
            data = cmg.format_response(False,None,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data


def twitter_acc_info(params):
        tokens = ast.literal_eval(params.tokens)
        id_list = ast.literal_eval(params.ids)
        try:
            api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
            data_ = Tw.get_account_summary(api, id_list)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except:
            data = cmg.format_response(False,None,'Error occurred while getting data from Twitter API',sys.exc_info())
        finally:
            return data

def build_word_cloud(params):

        tokens = ast.literal_eval(params.tokens)
        hash_tag = params.hashtag
        try:
            api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
            data_ = Tw.hashtag_search(api, hash_tag)
            wc_data = wc.wordcloud_json(data_)
            data = cmg.format_response(True,wc_data,'Data successfully processed!')
        except:
            data = cmg.format_response(False,None,'Error occurred while getting data from Twitter API',sys.exc_info())
        finally:
            return data

def build_word_cloud_fb(params):

        source = str(params.source)
        try:
            limit = params.limit
        except AttributeError:
            limit = ''
            pass
        try:
            since = params.since
        except AttributeError:
            since = ''
            pass
        try:
            until = params.until
        except AttributeError:
            until = ''
            pass
        try:
            post_ids = ast.literal_eval(params.post_ids)
        except AttributeError:
            post_ids = None
            pass
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        token = ast.literal_eval(params.token)

        data = FB.get_page_posts_comments(token, limit, since, until, page, post_ids)
        full_comment_str = ''
        full_comment = []
        analyzed_data = []

        if post_ids is not None:
            for post_id in post_ids:
                filtered_comments = filter(lambda d: d['post_id'] in post_id, data)
                for j in filtered_comments:
                   # full_comment.append(str(j['comments']))
                   p = j['comments']
                   for comment in j['comments']:

                       full_comment_str +=' '
                       full_comment_str += comment['message'].encode('UTF8')
                #print full_comment_str
                logger.info(full_comment_str)
                data_ = json.loads(wc.wordcloud_json(full_comment_str))
                full_comment_str = ''
                data_['post_id'] = post_id
                analyzed_data.append(data_)
            print analyzed_data
                #full_comment_str.join(full_comment)
                #analysed_data = sa.sentiment(full_comment_str.join(filtered_comments))
            data = cmg.format_response(True,analyzed_data,'Data successfully processed!')
            return data
        else:
            for post in data:
                for comments in post['comments']:
                   #comment = comments['message']
                   #full_comment.append(comment)
                    full_comment_str +=' '
                    full_comment_str += comments['message']
            analysed_data = wc.wordcloud_json(full_comment_str)
            data = cmg.format_response(True,analysed_data,'Data successfully processed!')
            return data


def build_word_cloud_rt(params):
        tokens = ast.literal_eval(params.tokens)
        hash_tag = str(params.hashtag)
        unique_id = str(params.unique_id)

        lsi.initialize_stream(hash_tag, unique_id, tokens) # if already exits do something
        data = smlf.process_social_media_data(unique_id, hash_tag)
        return data

def sentiment_analysis(params):

        tokens = ast.literal_eval(params.tokens)
        source = str(params.source)
        try:
            limit = params.limit
        except AttributeError:
            limit = ''
            pass
        try:
            since = params.since
        except AttributeError:
            since = ''
            pass
        try:
            until = params.until
        except AttributeError:
            until = ''
            pass
        try:
            post_ids = ast.literal_eval(params.post_ids)
        except AttributeError:
            post_ids = None
            pass
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        try:
            unique_id = str(params.unique_id)
            hash_tag = str(params.hash_tag)
        except AttributeError:
            hash_tag = ''
            pass
        analyzed_data = 'Incorrect datasource name provided!'
        if source == 'twitter':
            api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
            data_ = Tw.hashtag_search(api,hash_tag)
            #lsi.initialize_stream(hash_tag, unique_id, tokens) # if already exits do something
            #analyzed_data = smlf.process_social_media_data(unique_id, hash_tag)
            data = sa.sentiment(data_)
            result = cmg.format_response(True,data,'Data successfully processed!')
            return result

        elif source == 'facebook':
            data = FB.get_page_posts_comments(tokens, limit, since, until, page, post_ids)
            full_comment_str = ''
            full_comment = []
            analyzed_data = []

            if post_ids is not None:
                for post_id in post_ids:
                    filtered_comments = filter(lambda d: d['post_id'] in post_id, data)
                    for j in filtered_comments:
                       # full_comment.append(str(j['comments']))
                       p = j['comments']
                       for comment in j['comments']:

                           full_comment_str +=' '
                           full_comment_str += comment['message'].encode('UTF8')
                    #print full_comment_str
                    logger.info(full_comment_str)
                    data_ = sa.sentiment(full_comment_str)
                    full_comment_str = ''
                    data_['post_id'] = post_id
                    analyzed_data.append(data_)
                    #full_comment_str.join(full_comment)
                    #analysed_data = sa.sentiment(full_comment_str.join(filtered_comments))
                data = cmg.format_response(True,analyzed_data,'Data successfully processed!')
                return data

            else:
                for post in data:
                    for comments in post['comments']:
                       #comment = comments['message']
                       #full_comment.append(comment)
                        full_comment_str +=' '
                        full_comment_str += comments['message']
                analysed_data = sa.sentiment(full_comment_str)
                data = cmg.format_response(True,analysed_data,'Data successfully processed!')
                return data

def build_bi_partite(params):

        token= ast.literal_eval(params.token)
        source = str(params.source)
        try:
            limit = params.limit
        except AttributeError:
            limit = ''
            pass
        try:
            since = params.since
        except AttributeError:
            since = ''
            pass
        try:
            until = params.until
        except AttributeError:
            until = ''
            pass
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        posts_with_users = FB.get_page_posts_comments(token, limit, since, until, page, None)
        print json.dumps(posts_with_users)
        list_of_tuples = []
        tup = ()
        for post in posts_with_users:
            post_id = str(post['post_id'])
            if post['comments'] == []:
                tup = (post_id, '0')
                list_of_tuples.append(tup)
            for comment in post['comments']:
                user_name = comment['from']['name'] # TODO send userid to eliminate duplication
                tup = (post_id, user_name)
                list_of_tuples.append(tup)
                tup = ()
                print list_of_tuples
        analysed_data = bp.bipartite(list_of_tuples)
        data = cmg.format_response(True,analysed_data,'Data successfully processed!')
        return data


def test(params):
        token = params.token
        limit = ''
        since = ''
        until = ''
        post_ids = None
        page = 'me'
        try:
            post_ids = ast.literal_eval(params.post_ids)
            page = str(params.page)
            limit = params.limit
            since = params.since
            until = params.until
        except AttributeError:
            pass
        logger.info('Request received: %s' % params.values())
        data = FB.get_page_posts_comments(token, limit, since, until, page, post_ids)
        return json.dumps(data)

def streaming_tweets(params):
        data = Tw.get_streaming_tweets('123id', 'obama', 10)
        return data

