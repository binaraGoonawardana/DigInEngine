__author__ = 'Marlon Abeykoon'
__version__ =  'v3.1.0.0.8'

import sys,os
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)

import web
import json
import bigquery
from time import strftime
import logging
from multiprocessing import Process
import configs.ConfigHandler as conf
import modules.CommonMessageGenerator as comm

urls = (
    '/hierarchicalsummary(.*)', 'CreateHierarchicalSummary',
    '/gethighestlevel(.*)', 'GetHighestLevel',
    '/aggregatefields(.*)', 'AggregateFields',
    '/forecast(.*)', 'Forecasting',
    '/pageoverview(.*)', 'FBOverview',
    '/demographicsinfo(.*)', 'FBPageUserLocations',
    '/fbpostswithsummary(.*)', 'FBPostsWithSummary',
    '/promtionalinfo(.*)', 'FBPromotionalInfo',
    '/twitteraccinfo(.*)', 'TwitterAccInfo',
    '/buildwordcloudtwitter(.*)', 'BuildWordCloudTwitter',
    '/buildwordcloudrt(.*)', 'BuildWordCloudRT',
    '/streamingtweets(.*)', 'StreamingTweets',
    '/sentimentanalysis(.*)', 'SentimentAnalysis',
    '/buildwordcloudFB(.*)', 'BuildWordCloudFB',
    '/buildbipartite(.*)', 'BuildBiPartite',
    '/linear_regression(.*)', 'LinearRegression',
    '/file_upload(.*)', 'Upload',
    '/generateboxplot(.*)', 'BoxPlotGeneration',
    '/generatehist(.*)', 'HistogramGeneration',
    '/generatebubble(.*)', 'BubbleChart',
    '/executeQuery(.*)', 'ExecuteQuery',
    '/createDataset(.*)', 'CreateDataset',
    '/set_init_user_settings(.*)', 'SetInitialUserEnvironment',
    '/GetFields(.*)', 'GetFields',
    '/GetTables(.*)', 'GetTables',
    '/getLayout(.*)', 'GetLayout',
    '/getQueries(.*)','GetQueries',
    '/getreportnames(.*)','GetReportNames',
    '/executeKTR(.*)','ExecuteKTR',
    '/store_component(.*)','StoreComponent',
    '/get_all_components(.*)', 'GetAllComponents',
    '/get_component_by_category(.*)', 'GetComponentByCategory',
    '/get_component_by_comp_id(.*)', 'GetComponentByCompID',
    '/delete_components(.*)', 'DeleteComponents',
    '/store_user_settings(.*)', 'StoreUserSettings',
    '/get_user_settings(.*)', 'GetUserSettings',
    '/get_usage_summary(.*)', 'GetUsageSummary',
    '/get_usage_details(.*)', 'GetUsageDetails',
    '/clustering_kmeans(.*)', 'ClusteringKmeans',
    '/fuzzyc_calculation(.*)', 'ClusteringFuzzyc',
    '/share_components(.*)', 'ShareComponents',
    '/insert_data(.*)', 'InsertData',
    '/get_system_directories(.*)', 'GetSystemDirectories',
    '/clear_cache(.*)', 'ClearCache'
)
if __name__ == "__main__":
    print 'Starting...'
    print 'Loading configs...'
    cache_settings = conf.get_conf('CacheConfig.ini','Cache Expiration')
    default_cache_timeout = cache_settings['default_timeout_interval']
    ds_settings_mssql = conf.get_conf('DatasourceConfig.ini','MS-SQL')
    ds_settings_bq = conf.get_conf('DatasourceConfig.ini','BIG-QUERY')
    ds_settings_pg = conf.get_conf('DatasourceConfig.ini','PostgreSQL')
    ds_settings_mysql = conf.get_conf('DatasourceConfig.ini','MySQL')
    ds_settings_cache = conf.get_conf('DatasourceConfig.ini','MemSQL')
    ds_settings_auth = conf.get_conf('DatasourceConfig.ini','AUTH')
    path_settings_logs = conf.get_conf('FilePathConfig.ini','Logs')
    path_settings_ufiles = conf.get_conf('FilePathConfig.ini','User Files')
    print 'Configs loaded...'
    print 'Initializing logs...'
    path = path_settings_logs['Path']
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
    print 'Logs will be generated at %s' %path
    path = path_settings_ufiles['Path']
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
    print 'User files will be stored at %s' %path
    print 'Loading Engine modules and scripts...'
    import scripts
    print 'Modules and scripts loaded to the Engine...'
    print 'Checking datasource connections...'
    try:
        import modules.SQLQueryHandler as mssql
        print "Checking MSSQL connection at Server: " + ds_settings_mssql['SERVER'] + ' Port: ' + ds_settings_mssql['PORT']
        mssql.execute_query('SELECT 1')
        print "MSSQL connection successful!"
    except Exception, err:
        print err
        print "Error connecting to MSSQL server!"
    try:
        import modules.PostgresHandler as pg
        print "Checking PostgreSQL connection at Server: " + ds_settings_pg['HOST'] + ' Port: ' + ds_settings_pg['PORT']
        pg.execute_query('SELECT 1')
        print "PostgreSQL connection successful!"
    except Exception, err:
        print err
        print "Error connecting to pgSQL server!"
    try:
        import modules.BigQueryHandler as bq
        print "Checking BigQuery connection at Project: " + ds_settings_bq['PROJECT_ID'] + ' SERVICE_ACCOUNT: ' + ds_settings_bq['SERVICE_ACCOUNT']
        bq.execute_query('SELECT 1',user_id=0,tenant='DigInEngine')
        print "BigQuery connection successful!"
    except Exception, err:
        print err
        print "Error connecting to BigQuery server!"
    try:
        import modules.MySQLhandler as mysql
        print "Checking MySQL connection at Server: " + ds_settings_mysql['HOST'] + ' Port: ' + ds_settings_mysql['PORT']
        mysql.execute_query('SELECT 1','mysql')
        print "MySQL connection successful!"
    except Exception, err:
        print err
        print "Error connecting to MySQL!"
    try:
        print 'Initializing cache...'
        p = Process(target=scripts.DigINCacheEngine.CacheGarbageCleaner.initiate_cleaner)
        p.start()
        print 'Cache Initialized!'
    except Exception, err:
        print "Error occurred while initializing cache"
        print err
    try:
        print 'Initializing scheduled usage calculator'
        p2 = Process(target=scripts.DigInScheduler.DigInScheduler.DigInScheduler('UsageCalculatorJob','start').start_job())
        p2.start()
        print 'scheduled usage calculator initialized!'
    except Exception, err:
        print "Error occurred while initializing scheduled usage calculator"
        print err
    path_bq = os.path.dirname(bigquery.__file__)
    print 'NOTE: Add modified client.py of bigquery to the following path \n %s' %path_bq
    print(
    """

================================================================================
_____    _           _____             ______                   _
|  __ \  (_)         |_   _|           |  ____|                 (_)
| |  | |  _    __ _    | |    _ __     | |__     _ __     __ _   _   _ __     ___
| |  | | | |  / _` |   | |   | '_ \    |  __|   | '_ \   / _` | | | | '_ \   / _ \
| |__| | | | | (_| |  _| |_  | | | |   | |____  | | | | | (_| | | | | | | | |  __/
|_____/  |_|  \__, | |_____| |_| |_|   |______| |_| |_|  \__, | |_| |_| |_|  \___|
               __/ |                                      __/ |
              |___/                                      |___/
================================================================================

    """)
    print 'DigInEngine - ' + __version__ + ' started!'
    app = web.application(urls, globals())
    app.run()

Path_Settings = conf.get_conf('FilePathConfig.ini','Logs')
path = Path_Settings['Path']
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_path = path + '/DigInEngine.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

import scripts
#http://localhost:8080/hierarchicalsummary?h={%22vehicle_usage%22:1,%22vehicle_type%22:2,%22vehicle_class%22:3}&tablename=[digin_hnb.hnb_claims]&conditions=date%20=%20%272015-05-04%27%20and%20name=%27marlon%27&id=1


class CreateHierarchicalSummary(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received createHierarchicalSummary: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.LogicImplementer.LogicImplementer.create_hierarchical_summary(web.input(),md5_id,
                                                                                      json.loads(authResult.text)['UserID'],
                                                                                      json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed createHierarchicalSummary'
        return result

#http://localhost:8080/gethighestlevel?tablename=[Demo.Claims]&id=1&levels=[%27vehicle_usage%27,%27vehicle_class%27,%27vehicle_type%27]&plvl=All&SecurityToken=b1a1bdea465d4b3a0c70a45402ca8fd6&Domain=duosoftware.com&db=BigQuery


class GetHighestLevel(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_highest_level: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.LogicImplementer.LogicImplementer.get_highest_level(web.input(),md5_id,
                                                                                      json.loads(authResult.text)['UserID'],
                                                                                      json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_highest_level'
        return result

# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by=%{27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=sum&tablename=[digin_hnb.hnb_claims]&agg_f=[%27a3%27,%27b3%27,%27c3%27]&id=15&t=30000
# http://localhost:8080/aggregatefields?group_by={%27vehicle_type%27:1}&order_by={}&agg=sum&tablename=[digin_hnb.hnb_claims1]&agg_f=[%27claim_cost%27]&id=15&t=30000
# localhost:8080/aggregatefields?group_by={'a1':1,'b1':2,'c1':3}&order_by={'a2':1,'b2':2,'c2':3}&agg=[[%27field1%27,%27sum%27],[%27field2%27,%27avg%27]]&tablenames={1 : 'table1', 2:'table2', 3: 'table3'}&cons=a1=2&joins={1 : 'left outer join', 2 : 'inner join'}&join_keys={1: 'ON table1.field1' , 2: 'ON table2.field2'}&db=MSSQL&id=15&t=30000
# for Single table:
# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by={%27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=[[%27field1%27,%27sum%27],[%27field2%27,%27avg%27]]&tablenames={1%20:%20%27table1%27,%202:%27table2%27,%203:%20%27table3%27}&cons=a1=2&db=MSSQL&id=15&t=30000
# http://localhost:8080/aggregatefields?group_by={%27Category%27:1}&order_by={}&agg=[[%27Invoice_amount%27,%27sum%27]]&tablenames={1:%27Demo.epsi_sales%27}&cons=&db=BigQuery&id=15&t=30000&SecurityToken=0b4fac3276c5328db15e538590665d6a&Domain=duosoftware.com


class AggregateFields(web.storage):

    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received aggregate_fields: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.AggregationEnhancer.AggregationEnhancer.aggregate_fields(web.input(),md5_id,
                                                                                      json.loads(authResult.text)['UserID'],
                                                                                      json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed aggregate_fields'
        return result

#http://localhost:8080/forecast?model=Additive&pred_error_level=0.0001&alpha=0&beta=53&gamma=34&fcast_days=30&table_name=[Demo.forcast_superstoresales]&field_name_d=Date&field_name_f=Sales&steps_pday=1&m=7&interval=Daily
#http://localhost:8080/forecast?model=triple_exp&method=additive&alpha=0.716&beta=0.029&gamma=0.993&n_predict=24&table=[demo_duosoftware_com.Superstore]&date_field=Date&f_field=Sales&period=monthly&len_season=12&dbtype=bigquery&SecurityToken=2726315197e493f2b73b14a64940eeb6&Domain=digin.io


class Forecasting(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received Forecasting: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.PredictiveAnalysisEngine.ForecastingEsService.es_generation(web.input(),md5_id,
                                                                                      json.loads(authResult.text)['UserID'],
                                                                                      json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed Forecasting'
        return result

# http://localhost:8080/pageoverview?metric_names=['page_views']&token=CAACEdEosecBAMs8o7vZCgwsufVOQcLynVtFzCq6Ii1LwMyMRFgcV5xFPzUWGMKfJBJZBGb33yDciESrnThNY4mAV2fn14cGEjSUZAIvx0jMt4g6M3lEO8arfNPZBDISA49vO9F7LsKQwyePkWJBSN8NuMvaIWGzTfOrkpQzItLTlSSweUX8LOZB4TQRi8p8ZD&since=1447509660&until=1448028060


class FBOverview(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_overview: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_overview(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_overview'
        return result


class FBPageUserLocations(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_page_user_locations: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_page_user_locations(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_page_user_locations'
        return result

#http://localhost:8080/fbpostswithsummary?token=CAACEdEose0cBAJ6yiM46CxqzY3UyaTiSO2hwj451gctPwPULXdTQo8hmlufxjcKjTKySKQchoiRvBmodWivQ97tqTOmsfcZAt4b0ROwZCbFsrZAb3ED0sq3e4GbxL6OdjyAE26H9qBYs05msMm1uPdKRw2lLLfurThOKgTxdJwvTDgZBjMSxtJu6QYxLMcrgwgrzpJxZCQZDZD&limit=20&since=2015-07-01&until=2015-11-25


class FBPostsWithSummary(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_posts_with_summary: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_posts_with_summary(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_posts_with_summary'
        return result


class FBPromotionalInfo(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fb_promotional_info: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.fb_promotional_info(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed fb_promotional_info'
        return result

#http://localhost:8080/twitteraccinfo?ids=[%271123728482,5539932%27]&tokens={%27consumer_key%27:%27xHl7DEIJjH8pNM2kn8Q9EddGy%27,%27consumer_secret%27:%27eVxjTk7d4Z41VQ2Kt7kcOF6aFjTQqqiWIKgM8xhqkMYoE8Pxmq%27,%27access_token%27:%2779675949-r2z1UIBa5eeiIQBO6e4PSLytCMpfPUHC2lNoI7o2%27,%27access_token_secret%27:%27dBH5sLkief3oz7sftVwP30at1fij9dFm4hL02tpCUFxbj%27}


class TwitterAccInfo(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received twitter_acc_info: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.twitter_acc_info(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed twitter_acc_info'
        return result

#http://localhost:8080/hashtag?hashtag=%27%23get%27&tokens={%27consumer_key%27:%27xHl7DIJjH8pNM2kn8Q9EddGy%27,%27consumer_secret%27:%27xHl7DEIJjH8NM2kn8Q9EddGy%27,%27access_token%27:%2779675949-r2z1UIBa5eeiIQBO6e4PSL9ytCMpfPUHC2lNoI7o2%27,%27access_token_secret%27:%27dBH5sLkief3oz7sftVP30at1fij9dFm4hL02tpCUFxbj%27}


class BuildWordCloudTwitter(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_word_cloud: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_word_cloud(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_word_cloud'
        return result


#http://localhost:8080/buildwordcloudFB?token=%27CAACEdEose0cBAG6UIEov65x3tzohGZCON6UIAWInumkOZAInzw0ovs21Oh8090YV6hWP3pUTT853Q7wdSK8UfOCTqN68veN1bCnhWTn5hoZBnZBdI7vo4QMq5mtS5qZBJmfxnaZASiRfS9j2dlBmFqeWHd8faJrNij1QQasn22ZAcXMvB57KdbkWIUhTGxuvyQ1TTZBEewC9iQZDZD%27&hashtag=earthquake&unique_id=eq&source=facebook&post_ids=[%2710153455424900369%27]
class BuildWordCloudFB(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_word_cloud_fb: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_word_cloud_fb(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_word_cloud_fb'
        return result

#http://localhost:8080/buildwordcloudrt?tokens=%27rr%27&hashtag=earthquake&unique_id=eq


class BuildWordCloudRT(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_word_cloud_rt: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_word_cloud_rt(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_word_cloud_rt'
        return result

#http://localhost:8080/sentimentanalysis?tokens=%27CAACEdEose0cBAGDfAva3R79cV1CmNBSObNfAkZBz5Xbe4fGXN353jzynphA0ZBJ251mFce0CTJyZCSlfjQoIuuWJNJKrH6uQtNCeAWhOOZCWfX4VuuZBUvpx0QexOKMQG8E82Weqpi6wNziEXMJlzwGnhka1vbxCJZBt7vHHx4BDuUEWjWO3DZCbz3MbqfINbkZD%27&hashtag=earthquake&unique_id=eq&source=facebook&post_ids=[%27854964737921809_908260585925557%27,%27854964737921809_865555086862774%27]


class SentimentAnalysis(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received sentiment_analysis: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.sentiment_analysis(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed sentiment_analysis'
        return result

#http://localhost:8080/buildbipartite?token=%27CAACEdEose0cBAGDfAva3R79cV1CmNBSObNfAkZBz5Xbe4fGXN353jzynphA0ZBJ251mFce0CTJyZCSlfjQoIuuWJNJKrH6uQtNCeAWhOOZCWfX4VuuZBUvpx0QexOKMQG8E82Weqpi6wNziEXMJlzwGnhka1vbxCJZBt7vHHx4BDuUEWjWO3DZCbz3MbqfINbkZD%27&hashtag=earthquake&unique_id=eq&source=facebook&post_ids=[%27854964737921809_908260585925557%27,%27854964737921809_865555086862774%27]


class BuildBiPartite(web.storage):

    def GET(self, r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received build_bi_partite: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.SocialMediaService.SocialMediaService.build_bi_partite(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed build_bi_partite'
        return result

#http://localhost:8080/linear?dbtype=MSSQL&db=Demo&table=OrdersDK&x=Unit_Price&y=Sales&predict=[5,8]


class LinearRegression(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received linear_regression: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.DiginAlgo.DiginAlgo_service.linear_regression(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed linear_regression'
        return result


class Upload(web.storage):

    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        web.header('enctype','multipart/form-data')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received file_upload'
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            data_set_name = json.loads(authResult.text)['Email'].replace(".", "_").replace("@","_")
            result = scripts.FileUploadService.FileUploadService.file_upload(web.input(),web.input(file={}),data_set_name,
                                                                             json.loads(authResult.text)['UserID'],
                                                                             json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed file_upload'
        return result

class InsertData(web.storage):

    def OPTIONS(self, r):
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers',
                   'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self, r):
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers',
                   'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        web.header('enctype', 'multipart/form-data')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received file_upload'
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            data_set_name = json.loads(authResult.text)['Email'].replace(".", "_").replace("@", "_")
            result = scripts.FileUploadService.FileDatabaseInsertionCSV.csv_uploader(web.input(), data_set_name,
                                                                                     json.loads(authResult.text)['UserID'],
                                                                                     json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False, authResult.reason, "Check the custom message", exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed file_upload'
        return result


#http://localhost:8080/generateboxplot?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery


class BoxPlotGeneration(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received box_plot_generation: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received box_plot_generation: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.box_plot_generation(web.input(),md5_id,
                                                                             json.loads(authResult.text)['UserID'],
                                                                             json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed box_plot_generation'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed box_plot_generation')
        return result

#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&dbtype=BigQuery&ID=11
#http://localhost:8080/generatehist?q=[{%27[Demo.humanresource]%27:[%27Salary%27]}]&SecurityToken=7749e9d64eea8acf84bc3ee4368cec95&Domain=duosoftware.com


class HistogramGeneration(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received histogram_generation: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.histogram_generation(web.input(),md5_id,
                                                                             json.loads(authResult.text)['UserID'],
                                                                             json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed histogram_generation'
        return result

#http://localhost:8080/generatebubble?dbtype=BigQuery&db=Demo&table=humanresource&x=salary&y=Petrol_Allowance&s=salary&c=gender&ID=3


class BubbleChart(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received bubble_chart: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.DescriptiveAnalyticsService.DescriptiveAnalyticsService.bubble_chart(web.input(),md5_id,
                                                                             json.loads(authResult.text)['UserID'],
                                                                             json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed bubble_chart'
        return result


class ExecuteQuery(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received execute_query: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.DataSourceService.DataSourceService.execute_query(web.input(),md5_id,
                                                                                      json.loads(authResult.text)['UserID'],
                                                                                      json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed execute_query'
        return result


class CreateDataset(web.storage): # Deprecated
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received create_Dataset: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        result = scripts.DataSourceService.DataSourceService.create_Dataset(web.input())
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed create_Dataset'
        return result


class SetInitialUserEnvironment(web.storage):

    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        data = json.loads(web.data())
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received SetInitialUserEnvironment: Keys: {0}, values: {1}'\
            .format(data.keys(),data.values())
        secToken = web.ctx.env.get('HTTP_SECURITYTOKEN')
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.UserManagementService.UserMangementService.set_initial_user_env(data,
                                                                                             json.loads(authResult.text)['Email'],
                                                                                             json.loads(authResult.text)['UserID'],
                                                                                             json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed SetInitialUserEnvironment'
        return result


class GetFields(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_fields: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.DataSourceService.DataSourceService.get_fields(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_fields'
        return result


class GetTables(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_tables: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_tables: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.DataSourceService.DataSourceService.get_tables(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_tables'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_tables')
        return result


class GetLayout(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_layout: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.get_layout(web.input(),
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_layout'
        return result


class GetQueries(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_queries: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_queries: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.get_queries(web.input(),
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_queries'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_queries')
        return result


class GetReportNames(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_report_names: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Request received get_report_names: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values()))
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.get_report_names(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names')
        return result


class ExecuteKTR(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.PentahoReportingService.PentahoReportingService.executeKTR(web.input())
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_report_names')
        return result


class StoreComponent(web.storage):
    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        web.header("Content-Type", "applicatipn/json")
        secToken =  web.ctx.env.get('HTTP_SECURITYTOKEN')
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
             print json.loads(authResult.text)
             result = scripts.DiginComponentStore.DiginComponentStore.store_components(web.data(),
                                                                                       json.loads(authResult.text)['UserID'],
                                                                                       json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
             result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed store_component'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed store_component')
        return result


class GetAllComponents(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.DiginComponentStore.DiginComponentStore.get_all_components(web.input(),
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_all_components'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_all_components')
        return result


class GetComponentByCompID(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.DiginComponentStore.DiginComponentStore.get_component_by_comp_id(web.input(),
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_component_by_comp_id'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_component_by_comp_id')
        return result


class DeleteComponents(web.storage):
    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, DELETE, OPTIONS')

    def DELETE(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'true')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, DELETE, OPTIONS')
        secToken =  web.ctx.env.get('HTTP_SECURITYTOKEN')
        data = json.loads(web.data())
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
             result = scripts.DiginComponentStore.DiginComponentStore.delete_component(data,
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
             result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed delete_components'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed delete_components')
        return result


class StoreUserSettings(web.storage):

    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        data = json.loads(web.data())
        secToken =  web.ctx.env.get('HTTP_SECURITYTOKEN')
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.UserManagementService.UserMangementService.store_user_settings(data,
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed store_user_settings'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed store_user_settings')
        return result


class GetUserSettings(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.UserManagementService.UserMangementService.get_user_settings(json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_user_settings_by_id'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_user_settings_by_id')
        return result


class GetUsageSummary(web.storage):
    def GET(self, r):
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            security_level = scripts.utils.AuthHandler.get_security_level(secToken)
            result = scripts.DigInRatingEngine.DigInRatingEngine.RatingEngine(json.loads(authResult.text)['UserID'],
                                                                 json.loads(authResult.text)['Domain'],security_level).get_rating_summary()
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False, authResult.reason, "Check the custom message", exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_usage_summary'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_usage_summary')
        return result

class GetUsageDetails(web.storage):
        def GET(self, r):
            web.header('Access-Control-Allow-Origin', '*')
            web.header('Access-Control-Allow-Credentials', 'true')
            secToken = web.input().SecurityToken
            authResult = scripts.utils.AuthHandler.GetSession(secToken)
            if authResult.reason == "OK":
                security_level = scripts.utils.AuthHandler.get_security_level(secToken)
                result = scripts.DigInRatingEngine.DigInRatingEngine.RatingEngine(json.loads(authResult.text)['UserID'],
                                                                                  json.loads(authResult.text)['Domain'],
                                                                                  security_level).get_rating_detail(web.input())
            elif authResult.reason == 'Unauthorized':
                result = comm.format_response(False, authResult.reason, "Check the custom message", exception=None)
            print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_usage_details'
            logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_usage_details')
            return result

#http://localhost:8080/clustering_kmeans?data=[{%27demo_duosoftware_com.iris%27:[%27Sepal_Length%27,%27Petal_Length%27]}]&dbtype=bigquery&SecurityToken=ab46f8451d401be58d12eb5081660e80&Domain=duosoftware.com


class ClusteringKmeans(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received kmeans clustering: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.DiginAlgo.DiginAlgo_service.kmeans_calculation(web.input(), md5_id)
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed Kmeans Clustering'
        return result


#http://localhost:8080/fuzzyc_calculation?data=[{'demo_duosoftware_com.iris':['Sepal_Length','Petal_Length']}]&dbtype=bigquery&SecurityToken=ab46f8451d401be58d12eb5081660e80&Domain=duosoftware.com
class ClusteringFuzzyc(web.storage):
    def GET(self,r):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Request received fuzzyC clustering: Keys: {0}, values: {1}'\
            .format(web.input().keys(),web.input().values())
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            md5_id = scripts.utils.DiginIDGenerator.get_id(web.input(), json.loads(authResult.text)['UserID'])
            result = scripts.DiginAlgo.DiginAlgo_service.fuzzyc_calculation(web.input(), md5_id)
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed FuzzyC Clustering'
        return result


class ClearCache(web.storage):
    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

        secToken =  web.ctx.env.get('HTTP_SECURITYTOKEN')
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
             result = scripts.DigINCacheEngine.CacheController.clear_cache()
        elif authResult.reason == 'Unauthorized':
             result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed clear_cache'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed clear_cache')
        return result


class ShareComponents(web.storage):

    def OPTIONS(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def POST(self,r):
        web.header('Access-Control-Allow-Origin','*')
        web.header('Access-Control-Allow-Credentials', 'false')
        web.header('Access-Control-Allow-Headers', 'Content-Disposition, Content-Type, Packaging, Authorization, SecurityToken')
        web.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        data = json.loads(web.data())
        secToken =  web.ctx.env.get('HTTP_SECURITYTOKEN')
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            result = scripts.ShareComponentService.ShareComponentService.share_component(data,
                                                                                        json.loads(authResult.text)['UserID'],
                                                                                        json.loads(authResult.text)['Domain'])
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False,authResult.reason,"Check the custom message",exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed ShareComponents'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed ShareComponents')
        return result

class GetSystemDirectories(web.storage):
    def GET(self, r):
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        secToken = web.input().SecurityToken
        authResult = scripts.utils.AuthHandler.GetSession(secToken)
        if authResult.reason == "OK":
            OutPut = scripts.utils.GetSystemDirectories.get_folder_names(web.input(),json.loads(authResult.text)['UserID'],
                                                                 json.loads(authResult.text)['Domain'])
            result = comm.format_response(True, OutPut, "data_source_folders", exception=None)
        elif authResult.reason == 'Unauthorized':
            result = comm.format_response(False, authResult.reason, "Check the custom message", exception=None)
        print strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_system_directories'
        logger.info(strftime("%Y-%m-%d %H:%M:%S") + ' - Processing completed get_system_directories')
        return result