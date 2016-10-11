# -*- coding: utf-8 -*-
__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.1'

import pandas as pd
import json
import string


def string_formatter(raw_string):
    # Create string translation tables
    allowed = ' _01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    delchars = ''
    for i in range(255):
        if chr(i) not in allowed: delchars = delchars + chr(i)
    deltable = string.maketrans(' ','_')

    raw_string = str(string.strip(raw_string))
    raw_string = string.translate(raw_string,deltable,delchars)
    if raw_string == '': raw_string = 'undefined' # empty fieldnames are renamed as undefined
    fmtcol = ''
    for i in range(len(raw_string)):
        if raw_string.title()[i].isupper(): fmtcol = fmtcol + raw_string[i].upper()
        else: fmtcol = fmtcol + raw_string[i]
    fmtcol = fmtcol.lower()
    return fmtcol

def csv_schema_reader(file_path,filename,table_name,db):

    table_name = string_formatter(table_name)

    fileCsv = pd.read_csv(file_path+'/'+filename)
    columns = fileCsv.dtypes

    C = []
    for i in range(columns.size):
        if columns[i] == 'object':
            C.append(i)

    fileCsv = pd.read_csv(file_path+'/'+filename, parse_dates=C, infer_datetime_format=True)
    # print data.dtypes
    data_types = fileCsv.dtypes
    columnsDetails = data_types.to_frame(name='dataType')
    columnsDetails['headderName'] = fileCsv.columns.values.tolist()

    schema = []
    for i in range(len(columnsDetails.index)):
        field_detail = {'name': string_formatter(columnsDetails.iloc[i]['headderName']),
                        'type': columnsDetails.iloc[i]['dataType']}
        schema.append(field_detail)
    print schema

    print "Field type recognition successful"
    bq_schema = []
    if db.lower() == 'bigquery':

        # schema_dict = {}
        for i in schema:
            schema_dict = {}
            t = i['type']
            if t == 'object':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'string'
                schema_dict['mode'] = 'nullable'
            elif t == 'int64':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'integer'
                schema_dict['mode'] = 'nullable'
            elif t == 'float64':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'float'
                schema_dict['mode'] = 'nullable'
            elif t == 'datetime64[ns]':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'TIMESTAMP'
                schema_dict['mode'] = 'nullable'
            bq_schema.append(schema_dict)

    try:
        with open(file_path + '/schema.txt', 'w') as outfile:
            json.dump(bq_schema, outfile)
    except Exception, err:
        print err

    return bq_schema