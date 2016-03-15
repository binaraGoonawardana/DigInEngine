__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import sys
sys.path.append("...")
import modules.SQLQueryHandler as mssql
import modules.BigQueryHandler as bq
#import matplotlib.pyplot as plt

#http://localhost:8080/bubblechart?dbtype=BigQuery&db=Demo&table=humanresource&x=Salary&y=Petrol_Allowance&s=Salary&c=Job_Title
def bubblechart(dbtype, db, table, x, y, s, c):

    query = 'SELECT SUM({1}) x, SUM({2}) y, SUM({3}) s, {4} c From [{5}.{0}] Group BY c'.format(table, x, y, s, c,db)
    result = bq.execute_query(query)

    l = []; m = []; n = []; o = []
    for r in result:
        l.append(r['x']); m.append(r['y'])
        n.append(r['s']); o.append((r['c']))
    #print sum(n)
    data = {"x": l, "y": m, "s": n, "c": o}

    # count = collections.Counter(data['size'])
    # sizes = np.array(count.values())**2
    # plt.scatter(data['x'], data['y'], s=data['s'], c =data['c'], marker='o')
    # plt.show()

    return data


