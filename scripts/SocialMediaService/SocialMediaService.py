__author__ = 'Marlon Abeykoon'
__version__ = '1.2.3'

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
import threading
import ast
import logging
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/SocialMediaService.log'
handler = logging.FileHandler(log_path)
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
        except ValueError, err:
            data = cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
        except Exception, err:
            data = cmg.format_response(False,err,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data

def fb_page_user_locations(params):

        token = params.token
        logger.info('Requested received: %s' % params.values())
        try:
            data_ = FB.get_page_fans_city(token)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except ValueError, err:
            data = cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
            return data
        except Exception, err :
            data = cmg.format_response(False,err,'Error occurred while getting data from Facebook API',sys.exc_info())
            return data
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
        except ValueError, err:
            data = cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
        except Exception, err:
            data = cmg.format_response(False,err,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data

def fb_promotional_info(params):

        token = params.token
        promotional_name = params.metric_name
        try:
            data_ = FB.get_promotional_info(token, promotional_name)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except ValueError, err:
            data = cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
        except Exception, err:
            data = cmg.format_response(False,err,'Error occurred while getting data from Facebook API',sys.exc_info())
        finally:
            return data


def twitter_acc_info(params):
        tokens = ast.literal_eval(params.tokens)
        id_list = ast.literal_eval(params.ids)
        try:
            api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
            data_ = Tw.get_account_summary(api, id_list)
            data = cmg.format_response(True,data_,'Data successfully processed!')
        except Exception, err:
            data = cmg.format_response(False,err,'Error occurred while getting data from Twitter API',sys.exc_info())
        finally:
            return data

def build_word_cloud(params):

        tokens = ast.literal_eval(params.tokens)
        hash_tag = params.hash_tag
        try:
            api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
            data_ = Tw.hashtag_search(api, hash_tag)
            wc_data = json.loads(wc.wordcloud_json(data_))
            data = cmg.format_response(True,wc_data,'Data successfully processed!')
        except ValueError, err:
            data = cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
        except Exception, err:
            data = cmg.format_response(False,err,'Error occurred while getting data from Twitter API',sys.exc_info())
        finally:
            return data

def build_word_cloud_fb(params):

        #source = str(params.source)
        try:
            limit = params.limit
        except AttributeError:
            limit = ''
        try:
            since = params.since
        except AttributeError:
            since = ''
        try:
            until = params.until
        except AttributeError:
            until = ''
        try:
            post_ids = ast.literal_eval(params.post_ids)
        except AttributeError:
            post_ids = None
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        token = params.token

        try:
            data = FB.get_page_posts_comments(token, limit, since, until, page, post_ids)
        except ValueError, err:
            return cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
        full_comment_str = ''
        #full_comment = []
        analyzed_data = []

        if post_ids is not None:
            for post_id in post_ids:
                filtered_comments = filter(lambda d: d['post_id'] in post_id, data)
                for j in filtered_comments:
                   # full_comment.append(str(j['comments']))
                   #p = j['comments']
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
            try:
                analysed_data = json.loads(wc.wordcloud_json(full_comment_str))
            except ValueError:
                analysed_data = []
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

        try:
            tokens = ast.literal_eval(params.token)
        except ValueError:
            tokens = params.token
        source = str(params.source)
        try:
            limit = params.limit
        except AttributeError:
            limit = ''
        try:
            since = params.since
        except AttributeError:
            since = ''
        try:
            until = params.until
        except AttributeError:
            until = ''
        try:
            post_ids = ast.literal_eval(params.post_ids)
        except AttributeError:
            post_ids = None
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        try:
            hash_tag = str(params.hash_tag)
        except AttributeError:
            hash_tag = ''
        #analyzed_data = 'Incorrect datasource name provided!'
        if source == 'twitter':
            api = SMAuth.tweepy_auth(tokens['consumer_key'], tokens['consumer_secret'], tokens['access_token'], tokens['access_token_secret'])
            data_ = Tw.hashtag_search(api,hash_tag)
            #lsi.initialize_stream(hash_tag, unique_id, tokens) # if already exits do something
            #analyzed_data = smlf.process_social_media_data(unique_id, hash_tag)
            data = sa.sentiment(data_)
            result = cmg.format_response(True,data,'Data successfully processed!')
            return result

        elif source == 'facebook':
            try:
                data = FB.get_page_posts_comments(tokens, limit, since, until, page, post_ids)
            except ValueError, err:
                data = cmg.format_response(False,err,'Error validating access token: This may be because the user logged out or may be due to a system error.',sys.exc_info())
                return data

            #full_comment = []
            analyzed_data = []

            def _calculate_sentiment(post_id, comment_string):
                full_comment_str = ''
                for j in filtered_comments:
                    for comment in j['comments']:
                        full_comment_str += ' '
                        full_comment_str += comment['message'].encode('UTF8')
                logger.debug(full_comment_str)
                data_ = sa.sentiment(full_comment_str)
                full_comment_str = ''
                data_['post_id'] = post_id
                analyzed_data.append(data_)

            threads = []
            if post_ids is not None:
                for post_id in post_ids:
                    filtered_comments = filter(lambda d: d['post_id'] in post_id, data)
                    t = threading.Thread(target=_calculate_sentiment, args=(post_id, filtered_comments))
                    t.start()
                    print "Thread started to calculate sentiment analysis for post_id: {0}".format(post_id)
                    threads.append(t)
                    #full_comment_str.join(full_comment)
                    #analysed_data = sa.sentiment(full_comment_str.join(filtered_comments))
                for t in threads:
                    try:
                        t.join()
                    except Exception, err:
                        print err

                data = cmg.format_response(True,analyzed_data,'Data successfully processed!')
                return data

            else:
                for post in data:
                    full_comment_str = ''
                    for comments in post['comments']:
                        full_comment_str +=' '
                        full_comment_str += comments['message']
                analysed_data = sa.sentiment(full_comment_str)
                data = cmg.format_response(True,analysed_data,'Data successfully processed!')
                return data

def build_bi_partite(params):

        token= ast.literal_eval(params.token)
        #source = str(params.source)
        try:
            limit = params.limit
        except AttributeError:
            limit = ''
        try:
            since = params.since
        except AttributeError:
            since = ''
        try:
            until = params.until
        except AttributeError:
            until = ''
        page = 'me'
        try:
            page = str(params.page)
        except AttributeError:
            pass
        posts_with_users = FB.get_page_posts_comments(token, limit, since, until, page, None)
        print json.dumps(posts_with_users)
        list_of_tuples = []
        for post in posts_with_users:
            post_id = str(post['post_id'])
            if post['comments'] == []:
                tup = (post_id, '0')
                list_of_tuples.append(tup)
            for comment in post['comments']:
                user_name = comment['from']['name'] # TODO send userid to eliminate duplication
                tup = (post_id, user_name)
                list_of_tuples.append(tup)
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

