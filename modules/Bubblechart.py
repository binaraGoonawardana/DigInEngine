__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.1'

import sys
sys.path.append("...")
import logging
#import matplotlib.pyplot as plt
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/Bubblechart.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info('--------------------------------------  Bubble Chart  ------------------------------------------------------')
logger.info('Starting log')

#TODO bubble chart with qualitative x axis
#http://localhost:8080/generatebubble?&table=[demo_duosoftware_com.humanresource]&&x=Salary&&y=Medical_per_month&&c=Job_Title&&s=Salary&dbtype=BigQuery&SecurityToken=7c75d854afe251e01ac60decfd23781a&Domain=duosoftware.com
def bubblechart(result):

    # query = 'SELECT SUM({1}) x, SUM({2}) y, SUM({3}) s, {4} c From {0} Group BY c'.format(table, x, y, s, c,db)
    # result = bq.execute_query(query)
    logger.debug('data retrieved %s', result)
    l = []; m = []; n = []; o = []
    for r in result:
        l.append(r['x']); m.append(r['y'])
        n.append(r['s']); o.append((r['c']))
    #print sum(n)
    data = {"x": l, "y": m, "s": n, "c": o}
    logger.debug('return dataset created %s', data)
    # count = collections.Counter(data['size'])
    # sizes = np.array(count.values())**2
    # plt.scatter(data['x'], data['y'], s=data['s'], c =data['c'], marker='o')
    # plt.show()
    return data


