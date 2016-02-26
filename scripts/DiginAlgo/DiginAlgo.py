__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import sys
import json
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
import modules.PostgresHandler as postgres
import modules.linearRegression as l_reg
import modules.CommonMessageGenerator as cmg

def slr_get(dbtype, db, table,x,y,predict):

    #http://localhost:8080/linear?dbtype=MSSQL&db=Demo&table=OrdersDK&x=Unit_Price&y=Sales&predict=[5,8]
    if dbtype == 'MSSQL':

        try:
            query = 'SELECT {0} as x, {1} as y From {2}'.format(x, y, table)
            result = mssql.execute_query(query)
            print result

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from MSSQL!', sys.exc_info())
            return result

    #http://localhost:8080/linear?dbtype=BigQuery&db=Demo&table=forcast_superstoresales&x=UnitPrice&y=Sales&predict=[]
    elif dbtype == 'BigQuery':

        try:
            query = 'SELECT {0} as x, {1} as y From [{2}.{3}]'.format(x, y, db, table)
            result = BQ.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from BigQuery Handler!', sys.exc_info())
            return result

    #http://localhost:8080/linear?dbtype=pgSQL&db=HNBDB&table=hnb_gwp&x=basic&y=gwp&predict=[]
    elif dbtype == 'pgSQL':

        try:
            query = 'SELECT {0} as x, {1} as y From {2}'.format(x, y, table)
            print query
            """json.loads should remove after query handler change it"""
            result = postgres.execute_query(query)
            print result

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from Postgres Handler!', sys.exc_info())
            return result

    l = []
    m = []
    for r in result:
        l.append(r['x'])
        m.append(r['y'])

    predict = map(list,zip(*[iter(predict)]*1))

    data = {"x": l, "y": m, "predict": predict}

    input_x = map(list,zip(*[iter(data['x'])]*1))

    try:
        output = l_reg.simple_liner_regression(input_x, data['y'], data['predict'])
        output.update({'Actual_x': l, 'Actual_y': m})
        result = cmg.format_response(False,output,'Data processed successfully!')

    except Exception, err:

        result = cmg.format_response(False,None,'Algorithm Failed!', sys.exc_info())

    finally:
        return result



