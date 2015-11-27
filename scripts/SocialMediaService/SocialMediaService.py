__author__ = 'Marlon Abeykoon'

import FacebookAnalytics as fb
import json
import web
import ast
import logging

urls = (
    '/pageoverview(.*)', 'FBOverview',
    '/demographicsinfo(.*)', 'FBPageUserLocations',
    '/promtionalinfo(.*)', 'FBPromotionalInfo',
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

# http://localhost:8080/pageoverview?metric_names=['page_views']&token=CAACEdEose0cBAMs8o7vZCgwsufVOQcLynVtFzCq6Ii1LwMyMRFgcV5xFPzUWGMKfJBJZBGb33yDciESrnThNY4mAV2fn14cGEjSUZAIvx0jMt4g6M3lEO8arfNPZBDISA49vO9F7LsKQwyePkWJBSN8NuMvaIWGzTfOrkpQzItLTlSSweUX8LOZB4TQRi8p8ZD&since=1447509660&until=1448028060
class FBOverview(web.storage):
    def GET(self,r):

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
        data = json.dumps(fb.get_overview(token, metric_names, since, until))
        print data
        return data

class FBPageUserLocations(web.storage):
    def GET(selfself, r):
        token = web.input().token
        logger.info('Requested received: %s' % web.input().values())
        data = json.dumps(fb.get_page_fans_city(token))
        return data


class FBPromotionalInfo(web.storage):
    def GET(self, r):
        token = web.input().token
        promotional_name = web.input().metric_name
        data = json.dumps(fb.get_promotional_info(token,promotional_name))
        print data
        return data



if __name__ == "__main__":
    app.run()