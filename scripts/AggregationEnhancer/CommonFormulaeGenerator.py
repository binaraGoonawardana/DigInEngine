__author__ = 'Marlon Abeykoon'

import sys
sys.path.append("...")
import modules.BigQueryHandler as BQ
import modules.SQLQueryHandler as mssql

def percentage(db, table_name, field_name, conditions):

    if db == 'MSSQL':
        if conditions is None:
            query = 'SELECT {0}, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as "Percentage" FROM {1} GROUP BY {2}'\
                .format(field_name, table_name, field_name)
        else:
            query = 'SELECT {0}, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as "Percentage" FROM {1} WHERE {2} GROUP BY {3}'\
                .format(field_name, table_name, conditions, field_name)
        result = mssql.execute_query(query)
        return result
    else:
        #TODO for other DBs
        return 'Provided DB not supported'

# select Region, count(*) * 100.0 / sum(count(*)) over()
# from rep_factpolicybranch
# group by Region
