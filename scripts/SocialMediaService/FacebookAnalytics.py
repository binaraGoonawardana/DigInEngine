__author__ = 'Marlon Abeykoon'

import sys
sys.path.append("...")
import modules.SocialMediaAuthHandler as SMAuth
import logging
from time import gmtime, strftime


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('FacebookAnalytics.log')
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
        raise
    request_result['Total'] = summation
    return request_result


def get_page_posts(token, limit, since, until):
    page_auth = SMAuth.set_token(token)
    logger.info("Requesting data from API...")
    try:
        request_result = page_auth.request('me/posts',
                                           args={'limit': limit,
                                                 'since': since,
                                                 'until': until}
                                           )['data']
    except Exception, err:
        logger.error("Error occurred while requesting data from Graph API %s" % err)
        raise
    logger.debug('Data Received: %s' % request_result)

    output = []
    for complete_post in request_result:
        post = {'id': complete_post.get('id'),
                'message': complete_post.get('message'),
                'picture': complete_post.get('picture'),
                'likes': len([] if complete_post.get('likes', {}).get('data') is None
                             else complete_post.get('likes', {}).get('data')),
                'comments': len([] if complete_post.get('comments', {}).get('data') is None
                                else complete_post.get('comments', {}).get('data')),
                'shares': 0 if complete_post.get('shares', {}).get('count')is None
                else complete_post.get('shares', {}).get('count'),
                'created_time': complete_post.get('created_time')
                }

        output.append(post)

    logger.info("Data processed! Returned")
    return output


def get_promotional_info(token, promotion_node):
    page_auth = SMAuth.set_token(token)
    data = page_auth.get_connections("me", "Comments")
    return data
