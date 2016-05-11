__author__ = 'Manura Omal Bhagya'

import pandas as pd
import logging
import matplotlib as mpl
mpl.use('agg')
import configs.ConfigHandler as conf
#starttime = dt.asctime( dt.localtime(dt.time()))

#Create dataframe
#df = pd.DataFrame(np.random.rand(1000,10), columns=['A','B','C','D','E','F','G','H','I','J'])
# rec_data = [{'[digin_hnb.humanresource]':['age','salary']}]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/Boxplot.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('-----------------------------------------------------------------------------------------------------------------------')
logger.info('Starting log')


def boxplot(df):#TODO handle big data
    #get values for create boxplot
    logger.info('start processing')#Need matplotlib package to execute this
    try:
        _, bp = pd.DataFrame.boxplot(df, return_type='both')
    except Exception, err:
        logger.info(err)
        raise

    outliers = [flier.get_ydata() for flier in bp["fliers"]]
    boxes = [box.get_ydata() for box in bp["boxes"]]
    medians = [median.get_ydata() for median in bp["medians"]]
    whiskers = [whiskers.get_ydata() for whiskers in bp["whiskers"]]

    #converting ndarray to strings
    out_liers = [i.tolist() for i in outliers]

    logger.info('processed data : outliers = %s ,boxes = %s, medians = %s, whiskers = %s',out_liers,boxes,medians,whiskers)
    y = len(boxes)

    d = {}
    #arrange data into a dictionary format
    for i in range(0,y):
        d[list(df.columns.values)[i]] = {}
        d[list(df.columns.values)[i]]['quartile_1'] = boxes[i][4]
        d[list(df.columns.values)[i]]['quartile_2'] = medians[i][1]
        d[list(df.columns.values)[i]]['quartile_3'] = boxes[i][3]
        d[list(df.columns.values)[i]]['l_w'] = whiskers[i*2][1]
        d[list(df.columns.values)[i]]['u_w'] = whiskers[i*2+1][1]
        d[list(df.columns.values)[i]]['outliers'] = out_liers[i]

    #convert to json
    d_json = d
    logger.debug('Return json string : %s',d_json)
    return d_json

# def ret_data(rec_data):
#
#     df = pd.DataFrame()
#
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
#         #print query
#         #TODO add where clause for the query
#         logger.info('Query to retrieve data : %s',query)
#         #print query
#         try:
#             q = bq.execute_query(query)
#         except:
#             logger.error("Error fetching data from BQ")
#         logger.info('Data Recieved')
#         #print q
#         #shoud add real data to q
#         #q = [{u'count': 23, u'level': 66}, {u'count': 5, u'level': 84}, {u'count': 8, u'level': 267}]
#         #combine all the fields into a one dataframe
#         if df.empty:
#             df = pd.DataFrame(q)
#         else:
#             df1 = pd.DataFrame(q)
#             df = pd.concat([df,df1],axis=1)
#     #print df
#     logger.info('Data frame created.')
#     output_json = boxplot(df)
#     return output_json

#endtime = dt.asctime( dt.localtime(dt.time()))

#print starttime, '-----',endtime


# if  __name__ == "__main__":
#     ret_data(rec_data)