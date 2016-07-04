__author__ = 'Marlon Abeykoon'

import sys,os
import threading
#code added by sajee on 12/27/2015
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)

import modules.SocialMediaAuthHandler as SMAuth
import logging
from time import gmtime, strftime
import json
import requests
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/FacebookAnalytics.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('--------------------------------------  FacebookAnalytics  ---------------------------------------------')
logger.info('Starting log')


def get_overview(token, insight_nodes, since=None, until=None):
    """
    :param token: access_token for the page
    :param insight_nodes: Any node mentioned in https://developers.facebook.com/docs/graph-api/reference/v2.5/insights#page_users
    :param since: From date [Optional]
    :param until: To Date [Optional]
    :return: json
    """
    if insight_nodes is None:
        metrics = ["page_views", "page_stories", "page_fan_adds"]
    else:
        metrics = insight_nodes
    page_auth = SMAuth.set_token(token)
    output = []

    def dictionary_builder(ori_list, new_dict, name):
        data = []
        for line in new_dict:
            # append the new number to the existing array at this slot
            date_counts = [line['end_time'], line['value']]
            data.append(date_counts)
        ori_list.append({'name': name, 'data': data})
        return ori_list

    for i in metrics:
        try:
            if since is None or until is None:
                request_result = page_auth.request('me/insights/%s' % i)['data'][0]['values']


            else:
                request_result = page_auth.request('me/insights/%s' % i,
                                                 args={'since': since,
                                                      'until': until}
                                                )['data'][0]['values']
        except IndexError:
                request_result = [{'end_time': strftime("%Y-%m-%d %H:%M:%S", gmtime()), 'value': None}]
                pass
        except Exception, err:
            if err.message == ('Error validating access token: This may be because the user logged out or may be due to a system error.') \
                    or ('Error validating access token: The session is invalid because the user logged out.'):
                        raise ValueError(err)

        print request_result
        output = dictionary_builder(output, request_result, i)
    print output

    return output


def get_page_fans_city(token):
    page_auth = SMAuth.set_token(token)
    try:
        request_result = page_auth.request('me/insights/page_fans_city')['data'][0]['values'][0]
        summation = page_auth.request('me/insights/page_fans_country')['data'][0]['values'][0]['value']
    except Exception, err:
        logger.error("Error fetching data from API %s" % err)
        if err.message == ('Error validating access token: This may be because the user logged out or may be due to a system error.') \
                or ('Error validating access token: The session is invalid because the user logged out.'):
                    raise ValueError(err)
        raise
    request_result['Total'] = summation
    return request_result


def get_page_posts(token, limit, since, until, page='me'):
    page_auth = SMAuth.set_token(token)
    try:
        profile = page_auth.get_object(page)
    except Exception, err:
        if err.message == ('Error validating access token: This may be because the user logged out or may be due to a system error.') \
            or ('Error validating access token: The session is invalid because the user logged out.'):
                raise ValueError(err)
    logger.info("Requesting data from API...")
    profile_id = profile['id']
    #posts = page_auth.get_connections(profile['id'], 'posts')


    try:
        request_result = page_auth.request('{0}/posts'.format(profile_id),
                                           args={'limit': limit,
                                                 'since': since,
                                                 'until': until}
                                           )['data']
    except Exception, err:
        logger.error("Error occurred while requesting data from Graph API %s" % err)
        if err.message == ('Error validating access token: This may be because the user logged out or may be due to a system error.') \
                    or ('Error validating access token: The session is invalid because the user logged out.'):
                        raise ValueError(err)
        raise
    logger.debug('Data Received: %s' % request_result)

    output = []

    for complete_post in request_result:
        likes_count = page_auth.request(path='{0}/likes?summary=1'.format(complete_post.get('id')))["summary"]["total_count"]
        comments_count = page_auth.request(path='{0}/comments?summary=1'.format(complete_post.get('id')))["summary"]["total_count"]

        post = {'id': complete_post.get('id'),
                'message': complete_post.get('message'),
                'picture': complete_post.get('picture'),
                'likes': likes_count,
                'comments': comments_count,
                'shares': 0 if complete_post.get('shares', {}).get('count')is None
                else complete_post.get('shares', {}).get('count'),
                'created_time': complete_post.get('created_time')
                }

        output.append(post)

    logger.info("Data processed! Returned")
    return output

def get_page_posts_comments(token, limit, since, until, page='me', post_ids= None):
    page_auth = SMAuth.set_token(token)
    try:
        profile = page_auth.get_object(page)
    except Exception, err:
        if err.message == ('Error validating access token: This may be because the user logged out or may be due to a system error.') \
                    or ('Error validating access token: The session is invalid because the user logged out.'):
                        raise ValueError(err)
        raise
    logger.info("Requesting data from API...")
    profile_id = profile['id']
    if post_ids is None:
        try:
                request_result = page_auth.request('{0}/posts'.format(profile_id),
                                               args={'limit': limit,
                                                     'since': since,
                                                     'until': until}
                                               )['data']
                #print json.dumps(request_result)
        except Exception, err:
            logger.error("Error occurred while requesting data from Graph API %s" % err)
            raise
        logger.debug('Data Received: %s' % request_result)

        output = []
        for complete_post in request_result:  # Takes each post
            #print json.dumps(complete_post)
            partial_comments = [] if complete_post.get('comments') is None else complete_post.get('comments')
            comments_list = []
            while True:
                try:
                    for i in partial_comments.get('data'):
                        comments_list.append(i)
                    partial_comments = requests.get(partial_comments.get('paging').get('next')).json()
                except Exception:
                    break
            #print comments_list
            comments = {'post_id': complete_post.get('id'),
                        # 'comments': [] if complete_post.get('comments', {}).get('data') is None
                        #             else complete_post.get('comments', {}).get('data')
                        'comments': comments_list}
            output.append(comments)
            #print json.dumps(requests.get(complete_post.get('comments').get('paging').get('next')).json())

            #print partial_comments
        #print json.dumps(output)
        return output
    else:
        #posts = page_auth.get_objects(ids=post_ids)
        output = []
        for post_id in post_ids: # 854964737921809_908260585925557
            comments = page_auth.get_connections(id=post_id, connection_name='comments') #['data']
            #print json.dumps(comments)
            comments_list = []
            while True:
                try:
                    for i in comments.get('data'):
                        comments_list.append(i)
                    comments = requests.get(comments.get('paging').get('next')).json()
                except Exception:
                    break
            comments_with_postid = {'post_id': post_id,
                        'comments': comments_list}
            # for complete_post in posts:
            #     #print complete_post
            #     comment_details = {'message': '' if complete_post['message'] is None
            #                                     else complete_post['message'],
            #                     'id': complete_post.get('id')}
            # comments = {'post_id': post_id,
            #             #'comment_id': complete_post.get('id'),
            #             'comments': comment_details}
            output.append(comments_with_postid)
            #print json.dumps(comments_with_postid)
        #print json.dumps(output)
        return output
            # for post_id in post_ids:
            #     print posts
            #     print(posts[post_id]['data'])

def get_promotional_info(token, promotion_node):
    page_auth = SMAuth.set_token(token)
    data = page_auth.get_connections("me", "Comments")
    return data
