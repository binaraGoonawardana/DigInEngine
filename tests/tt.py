__author__ = 'Marlon'
import pandas as pd
# import operator
# group_bys_dict = {'a1':1,'b1':2,'c1':3}
# order_bys_dict = {'a2':1,'b2':2,'c2':3}
# aggregation_type = 'sum'
# tablename = 'tablename'
# aggregation_fields = ['a3','b3','c3']
#
# #SELECT a2, b2, c2, a1, b1, c1, sum(a3), sum(b3), sum(c3) FROM tablename GROUP BY a1, b1, c1 ORDER BY a2, b2, c2
#
#
#
# grp_tup = sorted(group_bys_dict.items(), key=operator.itemgetter(1))
# ordr_tup = sorted(order_bys_dict.items(), key=operator.itemgetter(1))
#
# group_bys_str = ''
# group_bys_str_ = ''
# if 1 in group_bys_dict.values():
#     group_bys = []
#     for i in range(0,len(grp_tup)):
#         group_bys.append(grp_tup[i][0])
#     print group_bys
#     group_bys_str_ = ', '.join(group_bys)
#     group_bys_str = 'GROUP BY %s' % ', '.join(group_bys)
#     print group_bys_str
#
# order_bys_str = ''
# order_bys_str_ = ''
# if 1 in order_bys_dict.values():
#     Order_bys = []
#     for i in range(0,len(ordr_tup)):
#         Order_bys.append(ordr_tup[i][0])
#     print Order_bys
#     order_bys_str_ = ', '.join(Order_bys)
#     order_bys_str = 'ORDER BY %s' % ', '.join(Order_bys)
#     print order_bys_str
#
# aggregation_fields_set = []
# for i in range(0,len(aggregation_fields)):
#     aggregation_fields_ = '{0}({1})'.format(aggregation_type, aggregation_fields[i])
#     aggregation_fields_set.append(aggregation_fields_)
# aggregation_fields_str = ', '.join(aggregation_fields_set)
#
# print aggregation_fields_str
#
# if 1 not in group_bys_dict.values():
#     fields_list = [order_bys_str_,aggregation_fields_str]
#
# elif 1 not in order_bys_dict.values():
#     fields_list = [group_bys_str_,aggregation_fields_str]
#
# else:
#     fields_list = [order_bys_str_,group_bys_str_,aggregation_fields_str]
#
# fields_str = ' ,'.join(fields_list)
# print fields_str
# query = 'SELECT {0} FROM {1}  {2}  {3}'.format(fields_str,tablename,group_bys_str,order_bys_str)
#
# print query

