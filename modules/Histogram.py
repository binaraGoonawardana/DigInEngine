__author__ = 'Manura Omal Bhagya'
import os,sys
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import logging
import numpy as np
import configs.ConfigHandler as conf
import math
import json

#serie = pd.read_csv('D:/sampledata/SuperstoreSales.csv', usecols = ['orderquantity'])
#rec_data = [{'[digin_hnb.humanresource]':['age']}]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/Histogram.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  HISTOGRAM  ------------------------------------------------------')
logger.info('Starting log')

#TODO add feature : customer to decide bin sizes
def histogram(df, n_bins = None):

    n = len(df)

    if n_bins == '':
        if n > 1000:  n_bins = 12
        elif n > 500: n_bins = 10
        elif n > 200: n_bins = 9
        elif n > 100: n_bins = 8
        elif n > 50:  n_bins = 7
        elif n > 20:  n_bins = 6
        else:
            n_bins = math.ceil(math.sqrt(n))

    logger.info('Number of data : count = %s,bin count = %s', n, n_bins)

    hist_data = np.histogram(df, bins=n_bins)
    frequency = list(hist_data[0])
    l_bound = list(hist_data[1])

    d = []
    for i in range(0,len(frequency)):
        #d[i] = [str(frequency[i]), str(l_bound[i]),str(l_bound[i+1])]
        d.append([str(frequency[i]), str(l_bound[i]),str(l_bound[i+1])])

    #dj = {"hist": d}

    return d

"""
def histogram(df):

    count,division = np.histogram(df)
    logger.info('histogram original data : count = %s,division = %s', count, division)

    ls = [division[i:i+2] for i in range(0,len(division))]

    lst = [i.tolist() for i in ls]
    logger.info('processed data : count = %s,bins = %s', count, lst)
    dict = {}

    # for c, b in zip(count, lst):
    #     if c not in dictionary: dictionary[c] = []
    #     dictionary[c].append(b)
    # dictionary = str(dictionary)

    dict = {str(key): value for (key, value) in zip(lst, count)}
    dictionary = {}
    i = 0
    for k,v in dict.items():
        i += 1
        dictionary[i] = {k:v}

    d_json = ''
    logger.info('dictioanry Created %s',dictionary)
    try:
        #d_json = json.dumps(dictionary, ensure_ascii=False)
        d_json = dictionary
        logger.debug('Json Created %s',d_json)
    except Exception, err:
        logger.info(err)

    return d_json
"""
#rec_data = [{'table1':['A']}]

# def ret_data(rec_data):
#
#     df = pd.DataFrame()
#     db = ''
#     for i in range(0,len(rec_data)):
#         tables = rec_data[i].keys()
#         fields = rec_data[i].values()
#         fields = fields[0]
#
#         fields_str = ', '.join(fields)
#         tables_str = ', '.join(tables)
#
#         #get data from many tables
#         query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
#         logger.info('Query to retrieve data : %s',query)
#
#         q = bq.execute_query(query)
#         #q = da.data_ret(dbtype, db, tables, fields)
#         logger.info('Data Received %s', q)
#
#         #q = [{u'count': 12}, {u'count': 5}, {u'count': 8},{u'count': 11},{u'count': 10},{u'count': 2},{u'count': 9},{u'count': 5},]
#         #combine all the fields into a one dataframe
#
#         if df.empty:
#             df = pd.DataFrame(q)
#         else:
#             df1 = pd.DataFrame(q)
#             df = pd.concat([df,df1],axis=1)
#
#     logger.info('Data frame created.')
#     output_json = histogram(df)
#     return output_json
