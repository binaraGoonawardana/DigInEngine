__author__ = 'Marlon Abeykoon'


def get_func(db, field_name, aggregator):

    if aggregator.lower() == 'percentage':
        if db == 'MSSQL':
            func = 'COUNT({0}) * 100.0 / SUM(COUNT({1})) OVER() as percentage_{2}'.format(field_name, field_name,field_name )
            return func
    if aggregator.lower() == 'avg':
        func = 'avg({0}) as avg_{1}'.format(field_name, field_name)
        return func
    if aggregator.lower() == 'sum':
        func = 'sum({0}) as sum_{1}'.format(field_name, field_name)
        return func
    if aggregator.lower() == 'count':
        func = 'count({0}) as count_{1}'.format(field_name, field_name)
        return func
    if aggregator.lower() == 'max':
        func = 'max({0}) as max_{1}'.format(field_name, field_name)
        return func
    if aggregator.lower() == 'min':
        func = 'min({0}) as min_{1}'.format(field_name, field_name)
        return func
