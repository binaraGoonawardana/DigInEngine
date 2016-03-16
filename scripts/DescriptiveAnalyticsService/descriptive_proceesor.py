__author__ = 'Manura Omal Bhagya'
__version__ = '1.0.0.0'

import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql
#import modules.PostgresHandler as postgres
import modules.CommonMessageGenerator as cmg
import modules.Bubblechart as Bubble
import modules.Histogram as Hist
import modules.Boxplot as Box
import pandas as pd

def ret_hist(dbtype, rec_data, type):

    df = pd.DataFrame()
    for i in range(0,len(rec_data)):
        tables = rec_data[i].keys()
        fields = rec_data[i].values()
        fields = fields[0]

        fields_str = ', '.join(fields)
        tables_str = ', '.join(tables)

        if dbtype == 'MSSQL':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
                result = mssql.execute_query(query)

            except Exception, err:
                result = cmg.format_response(False, None, 'Error occurred while getting data from MSSQL!', sys.exc_info())
                return result

        elif dbtype == 'BigQuery':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
                result = BQ.execute_query(query)

            except Exception, err:
                result = cmg.format_response(False, None, 'Error occurred while getting data from BigQuery Handler!', sys.exc_info())
                return result

        elif dbtype == 'pgSQL':

            try:
                query = 'SELECT {0} FROM {1}'.format(fields_str,tables_str)
                result = postgres.execute_query(query)

            except Exception, err:
                result = cmg.format_response(False, None, 'Error occurred while getting data from Postgres Handler!', sys.exc_info())
                return result

        if df.empty:
            df = pd.DataFrame(result)
        else:
            df1 = pd.DataFrame(result)
            df = pd.concat([df,df1],axis=1)

    if type == "hist":
        try:
            output = Hist.histogram(df)
            result = cmg.format_response(True,output,'Histogram processed successfully!')

        except Exception, err:

            result = cmg.format_response(False,None,'Histogram Failed!', sys.exc_info())

        finally:
            return result

    elif type == "box":
        try:
            output = Box.boxplot(df)
            result = cmg.format_response(True,output,'Boxplot processed successfully!')

        except Exception, err:

            result = cmg.format_response(False,None,'Boxplot Failed!', sys.exc_info())

        finally:
            return result

def ret_bubble(dbtype, db, table, x, y, s, c):

    if dbtype == 'MSSQL':

        try:
            query = 'SELECT SUM({1}) x, SUM({2}) y, SUM({3}) s, {4} c From {0} Group BY c'.format(table, x, y, s, c,db)
            result = mssql.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from MSSQL!', sys.exc_info())
            return result

    elif dbtype == 'BigQuery':

        try:
            query = 'SELECT SUM({1}) x, SUM({2}) y, SUM({3}) s, {4} c From [{5}.{0}] Group BY c'.format(table, x, y, s, c, db)
            result = BQ.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from BigQuery Handler!', sys.exc_info())
            return result

    elif dbtype == 'pgSQL':

        try:
            query = 'SELECT SUM({1}) x, SUM({2}) y, SUM({3}) s, {4} c From {0} Group BY c'.format(table, x, y, s, c,db)
            #result = postgres.execute_query(query)

        except Exception, err:
            result = cmg.format_response(False, None, 'Error occurred while getting data from Postgres Handler!', sys.exc_info())
            return result

    try:
        output = Bubble.bubblechart(result)
        result = cmg.format_response(True,output,'Bubblechart processed successfully!')

    except Exception, err:
        result = cmg.format_response(False,None,'Bubblechart Failed!', sys.exc_info())

    finally:
        return result
