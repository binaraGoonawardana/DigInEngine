__author__ = 'Marlon'

import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import scripts.DigINCacheEngine.CacheController as CC
import web
import logging
import operator
import ast
import json

urls = (
    '/aggregatefields(.*)', 'AggregateFields'
)

app = web.application(urls, globals())

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('AggregationEnhancer.log')
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')
#http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by={%27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=sum&tablename=[digin_hnb.hnb_claims]&agg_f=[%27a3%27,%27b3%27,%27c3%27]
#http://localhost:8080/aggregatefields?group_by={%27vehicle_type%27:1}&order_by={}&agg=sum&tablename=[digin_hnb.hnb_claims1]&agg_f=[%27claim_cost%27]
class AggregateFields():
    def GET(self,r):

        group_bys_dict = ast.literal_eval(web.input().group_by) #{'a1':1,'b1':2,'c1':3}
        order_bys_dict = {}
        try:
            order_bys_dict = ast.literal_eval(web.input().order_by) #{'a2':1,'b2':2,'c2':3}
        except:
            logger.info("No order by clause")
        aggregation_type = web.input().agg #'sum'
        tablename = web.input().tablename #'tablename'
        aggregation_fields = ast.literal_eval(web.input().agg_f) #['a3','b3','c3']
        try:
            conditions = web.input().cons
        except AttributeError:
            logger.info("No Where clause found")
            conditions = ''
            pass
        db = web.input().db

        #SELECT a2, b2, c2, a1, b1, c1, sum(a3), sum(b3), sum(c3) FROM tablename GROUP BY a1, b1, c1 ORDER BY a2, b2, c2

        grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))


        group_bys_str = ''
        group_bys_str_ = ''
        if 1 in group_bys_dict.values():
            group_bys = []
            for i in range(0,len(grp_tup)):
                group_bys.append(grp_tup[i][0])
            print group_bys
            group_bys_str_ = ', '.join(group_bys)
            group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
            print group_bys_str

        if order_bys_dict != {}:
            ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
            order_bys_str = ''
            order_bys_str_ = ''
            if 1 in order_bys_dict.values():
                Order_bys = []
                for i in range(0,len(ordr_tup)):
                    Order_bys.append(ordr_tup[i][0])
                print Order_bys
                order_bys_str_ = ', '.join(Order_bys)
                order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                print order_bys_str

        else:
            order_bys_str = ''

        aggregation_fields_set = []
        for i in range(0,len(aggregation_fields)):
            aggregation_fields_ = 'str({0}({1})) agg'.format(aggregation_type, aggregation_fields[i]) # aggregation_fields_ = '{0}({1})'.format(aggregation_type, aggregation_fields[i])
            aggregation_fields_set.append(aggregation_fields_)
        aggregation_fields_str = ', '.join(aggregation_fields_set)

        print aggregation_fields_str

        if 1 not in group_bys_dict.values():
            fields_list = [order_bys_str_,aggregation_fields_str]

        elif 1 not in order_bys_dict.values():
            fields_list = [group_bys_str_,aggregation_fields_str]

        else:
            fields_list = [order_bys_str_,group_bys_str_,aggregation_fields_str]

        fields_str = ' ,'.join(fields_list)
        print fields_str
        query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str,tablename,conditions,group_bys_str,order_bys_str)
        logger.info('Query formed successfully! : %s' %query)
        logger.info('Fetching data from BigQuery...')
        result = ''
        if db == 'BQ':
            try:
                result = BQ.execute_query(query)
                logger.info('Data received!')
                logger.debug('Result %s' %result)
            except Exception, err:
                logger.error('Error occurred while getting data from BigQuery Handler!')
        elif db == 'MSSQL':
            try:
                result = mssql.execute_query(query)
                logger.info('Data received!')
                logger.debug('Result %s' %result)
            except Exception, err:
                logger.error('Error occurred while getting data from sql Handler!')
        else:
            #TODO implement for other dbs
            logger.error('DB not supported')
            raise
        result_dict = json.loads(result)
        print result_dict
        return json.dumps(result_dict)


if  __name__ == "__main__":
    app.run()