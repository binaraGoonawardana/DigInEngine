__author__ = 'Marlon Abeykoon'

import sys
sys.path.append("...")
import modules.SocialMediaAuthHandler as SMAuth
import logging


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
            print line['end_time']
            print type(data)
            date_counts = [line['end_time'], line['value']]
            data.append(date_counts)
            print data
        ori_list.append({'name': name, 'data': data})
        return ori_list

    for i in metrics:
        if since is None or until is None:
            request_result = page_auth.request('me/insights/%s' % i)['data'][0]['values']
        else:
            request_result = page_auth.request('me/insights/%s' % i,
                                               args={'since': since,
                                                     'until': until}
                                               )['data'][0]['values']
        print request_result
        output = dictionary_builder(output, request_result, i)
    print output

    return output


def get_page_fans_city(token):
    page_auth = SMAuth.set_token(token)
    request_result = page_auth.request('me/insights/page_fans_city')['data'][0]['values'][1]
    summation = page_auth.request('me/insights/page_fans_country')['data'][0]['values'][1]['value']
    request_result['Total'] = summation
    return request_result


def get_promotional_info(token, promotion_node):
    page_auth = SMAuth.set_token(token)
    data = page_auth.get_connections("me", "Comments")
    return data
