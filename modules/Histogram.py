__author__ = 'Manura Omal Bhagya'

import numpy as np
import pandas as pd
import json
import logging
import itertools
import BigQueryHandler as bq
import sys


#serie = pd.read_csv('D:/sampledata/SuperstoreSales.csv', usecols = ['orderquantity'])
#rec_data = [{'[digin_hnb.humanresource]':['age']}]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('histogram.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  HISTOGRAM  ------------------------------------------------------')
logger.info('Starting log')

#TODO add feature : customer to decide bin sizes
def histogram(df):

    count,division = np.histogram(df)
    logger.info('histogram original data : count = %s,division = %s',count,division)

    ls = [division[i:i+2] for i in range(0,len(division))]
    lst = [i.tolist() for i in ls]
    logger.info('processed data : count = %s,bins = %s',count,lst)
    dictionary = {}
    for c, b in zip(count, lst):
        if c not in dictionary: dictionary[c] = []
        dictionary[c].append(b)
    dictionary = str(dictionary)

    # dictionary = str({str(key): value for (key, value) in zip(lst, count)})

    d_json = ''
    logger.info('dictioanry Created %s',dictionary)
    try:
        d_json = json.dumps(dictionary, ensure_ascii=False)
        logger.info('Json Created %s',d_json)
    except Exception, err:
        logger.info(err)

    return d_json

#rec_data = [{'table1':['A']}]

def ret_data(rec_data):

    df = pd.DataFrame()

    for i in range(0,len(rec_data)):
        tables = rec_data[i].keys()
        fields = rec_data[i].values()
        fields = fields[0]

        fields_str = ', '.join(fields)
        tables_str = ', '.join(tables)

        #get data from many tables
        query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
        logger.info('Query to retrieve data : %s',query)

        q = json.loads(bq.execute_query(query))
        logger.info('Data Recieved %s', q)

        #q = [{u'count': 12}, {u'count': 5}, {u'count': 8},{u'count': 11},{u'count': 10},{u'count': 2},{u'count': 9},{u'count': 5},]
        #combine all the fields into a one dataframe

        if df.empty:
            df = pd.DataFrame(q)
        else:
            df1 = pd.DataFrame(q)
            df = pd.concat([df,df1],axis=1)

    logger.info('Data frame created.')
    output_json = histogram(df)
    return output_json
