__author__ = 'Marlon Abeykoon'
__version__ = '1.2.0.0'

import CommonFormulaeGenerator as cfg
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
# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by=%{27a2%27:1,%27b2%27:2,%27c2%27:3}&agg=sum&tablename=[digin_hnb.hnb_claims]&agg_f=[%27a3%27,%27b3%27,%27c3%27]
# http://localhost:8080/aggregatefields?group_by={%27vehicle_type%27:1}&order_by={}&agg=sum&tablename=[digin_hnb.hnb_claims1]&agg_f=[%27claim_cost%27]
# localhost:8080/aggregatefields?group_by={'a1':1,'b1':2,'c1':3}&order_by={'a2':1,'b2':2,'c2':3}&agg={'field1' : 'sum', 'field2' : 'avg'}&tablenames={1 : 'table1', 2:'table2', 3: 'table3'}&cons=a1=2&joins={1 : 'left outer join', 2 : 'inner join'}&join_keys={1: 'ON table1.field1' , 2: 'ON table2.field2'}&db=MSSQL
# for Single table:
# http://localhost:8080/aggregatefields?group_by={%27a1%27:1,%27b1%27:2,%27c1%27:3}&order_by={%27a2%27:1,%27b2%27:2,%27c2%27:3}&agg={%27field1%27%20:%20%27sum%27,%20%27field2%27%20:%20%27avg%27}&tablenames={1%20:%20%27table1%27,%202:%27table2%27,%203:%20%27table3%27}&cons=a1=2&db=MSSQL
class AggregateFields():
    def GET(self, r):

        group_bys_dict = ast.literal_eval(web.input().group_by)  # {'a1':1,'b1':2,'c1':3}
        order_bys_dict = ast.literal_eval(web.input().order_by)  # {'a2':1,'b2':2,'c2':3}
        tablenames = ast.literal_eval(web.input().tablenames)  # 'tablenames' {1 : 'table1', 2:'table2', 3: 'table3'}
        aggregations = ast.literal_eval(web.input().agg) # {'field1' : 'sum', 'field2 : 'avg'}
        conditions = web.input().cons # ''
        try:
            join_types = ast.literal_eval(web.input().joins) # {1 : 'left outer join', 2 : 'inner join'}
            join_keys = ast.literal_eval(web.input().join_keys) # {1: 'ON table1.field1' , 2: 'ON table2.field2'}
        except AttributeError:
            logger.info("Single table query received")
            join_types = {}
            join_keys = {}
            pass
        db = web.input().db

        # SELECT a2, b2, c2, a1, b1, c1, sum(a3), sum(b3), sum(c3) FROM tablenames GROUP BY a1, b1, c1 ORDER BY a2, b2, c2

        if db == 'MSSQL':
            logger.info("MSSQL - Processing started!")
            if aggregations.itervalues().next() != 'percentage':
                query_body = tablenames[1]
                if join_types and join_keys != {}:
                    for i in range(0, len(join_types)):
                        sub_join_body = join_types[i+1] + ' ' + tablenames[i+2] + ' ' + join_keys[i+1]
                        query_body += ' '
                        query_body += sub_join_body

                if conditions:
                    conditions = 'WHERE %s' %(conditions)

                if group_bys_dict != {}:
                    logger.info("Group by statement creation started!")
                    grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))
                    group_bys_str = ''
                    group_bys_str_ = ''
                    if 1 in group_bys_dict.values():
                        group_bys = []
                        for i in range(0, len(grp_tup)):
                            group_bys.append(grp_tup[i][0])
                        group_bys_str_ = ', '.join(group_bys)
                        group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
                    logger.info("Group by statement creation completed!")

                else:
                    group_bys_str = ''
                    group_bys_str_ = ''

                if order_bys_dict != {}:
                    logger.info("Order by statement creation started!")
                    ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
                    order_bys_str = ''
                    order_bys_str_ = ''
                    if 1 in order_bys_dict.values():
                        Order_bys = []
                        for i in range(0, len(ordr_tup)):
                            Order_bys.append(ordr_tup[i][0])
                        order_bys_str_ = ', '.join(Order_bys)
                        order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                    logger.info("Order by statement creation completed!")

                else:
                    order_bys_str = ''

                logger.info("Select statement creation started!")
                aggregation_fields_set = []
                for key, value in aggregations.iteritems():
                    altered_field = key.replace('.','_')
                    aggregation_fields = '{0}({1}) as {2}_{3}'.format(value, key, value, altered_field)
                    aggregation_fields_set.append(aggregation_fields)
                aggregation_fields_str = ', '.join(aggregation_fields_set)

                if 1 not in group_bys_dict.values() and 1 in order_bys_dict.values():
                    fields_list = [order_bys_str_, aggregation_fields_str]

                elif 1 not in order_bys_dict.values() and 1 in group_bys_dict.values():
                    fields_list = [group_bys_str_, aggregation_fields_str]

                elif 1 not in group_bys_dict.values() and 1 not in order_bys_dict.values():
                    fields_list = [aggregation_fields_str]

                else:
                    intersect_groups_orders = group_bys
                    intersect_groups_orders.extend(x for x in Order_bys if x not in intersect_groups_orders)
                    fields_list = intersect_groups_orders + [aggregation_fields_str]

                fields_str = ' ,'.join(fields_list)
                logger.info("Select statement creation completed!")
                query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str, query_body, conditions, group_bys_str,
                                                                 order_bys_str)
                print query
                logger.info('Query formed successfully! : %s' % query)
                logger.info('Fetching data from SQL...')
                result = ''

                try:
                    result = mssql.execute_query(query)
                    logger.info('Data received!')
                    logger.debug('Result %s' % result)
                except Exception, err:
                    logger.error('Error occurred while getting data from sql Handler!')
                    logger.error(err)

                result_dict = json.loads(result)
                logger.info("MSSQL - Processing completed!")
                return json.dumps(result_dict)

            elif aggregations.itervalues().next() == 'percentage':
                result = cfg.percentage('MSSQL', tablenames[1],aggregations.iterkeys().next(),None)
                return result

        elif db == 'BQ':

            logger.info("BQ - Processing started!")
            query_body = tablenames[1]
            if join_types and join_keys != {}:
                for i in range(0, len(join_types)):
                    sub_join_body = join_types[i+1] + ' ' + tablenames[i+2] + ' ' + join_keys[i+1]
                    query_body += ' '
                    query_body += sub_join_body

            if conditions:
                conditions = 'WHERE %s' %(conditions)

            if group_bys_dict != {}:
                logger.info("Group by statement creation started!")
                grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))

                group_bys_str = ''
                group_bys_str_ = ''
                if 1 in group_bys_dict.values():
                    group_bys = []
                    for i in range(0, len(grp_tup)):
                        group_bys.append(grp_tup[i][0])
                    group_bys_str_ = ', '.join(group_bys)
                    group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
                logger.info("Group by statement creation completed!")
            else:
                group_bys_str = ''
                group_bys_str_ = ''

            if order_bys_dict != {}:
                logger.info("Order by statement creation started!")
                ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
                order_bys_str = ''
                order_bys_str_ = ''
                if 1 in order_bys_dict.values():
                    Order_bys = []
                    for i in range(0, len(ordr_tup)):
                        Order_bys.append(ordr_tup[i][0])
                    order_bys_str_ = ', '.join(Order_bys)
                    order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
                logger.info("Order by statement creation completed!")

            else:
                order_bys_str = ''

            logger.info("Select statement creation started!")
            aggregation_fields_set = []
            for key, value in aggregations.iteritems():
                altered_field = key.replace('.','_')
                aggregation_fields = '{0}({1}) as {2}_{3}'.format(value, key, value, altered_field)
                aggregation_fields_set.append(aggregation_fields)
            aggregation_fields_str = ', '.join(aggregation_fields_set)

            if 1 not in group_bys_dict.values() and 1 in order_bys_dict.values():
                fields_list = [order_bys_str_, aggregation_fields_str]

            elif 1 not in order_bys_dict.values() and 1 in group_bys_dict.values():
                fields_list = [group_bys_str_, aggregation_fields_str]

            elif 1 not in group_bys_dict.values() and 1 not in order_bys_dict.values():
                fields_list = [aggregation_fields_str]

            else:
                intersect_groups_orders = group_bys
                intersect_groups_orders.extend(x for x in Order_bys if x not in intersect_groups_orders)
                fields_list = intersect_groups_orders + [aggregation_fields_str]

            fields_str = ' ,'.join(fields_list)

            logger.info("Select statement creation started!")

            query = 'SELECT {0} FROM {1} {2} {3} {4}'.format(fields_str, query_body, conditions, group_bys_str,
                                                             order_bys_str)
            print query
            logger.info('Query formed successfully! : %s' % query)
            logger.info('Fetching data from SQL...')
            result = ''

            try:
                result = BQ.execute_query(query)
                logger.info('Data received!')
                logger.debug('Result %s' % result)
            except Exception, err:
                logger.error('Error occurred while getting data from BQ Handler!')
                logger.error(err)
            result_dict = json.loads(result)
            logger.info("BQ - Processing completed!")
            return json.dumps(result_dict)


if __name__ == "__main__":
    app.run()
