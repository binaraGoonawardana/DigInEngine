# -*- coding: utf-8 -*-
__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.5'

import pandas as pd
import json
import string
import numpy as np

def string_formatter(raw_string,j):
    # Create string translation tables
    allowed = ' _01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    delchars = ''
    for i in range(255):
        if chr(i) not in allowed: delchars = delchars + chr(i)
    deltable = string.maketrans(' ','_')

    raw_string = str(string.strip(raw_string))
    raw_string = string.translate(raw_string,deltable,delchars)
    if raw_string == ''or raw_string.isdigit(): raw_string = 'unnamed_'+str(j)# empty fieldnames are renamed as unnamed_
    fmtcol = ''
    for i in range(len(raw_string)):
        if raw_string.title()[i].isupper(): fmtcol = fmtcol + raw_string[i].upper()
        else: fmtcol = fmtcol + raw_string[i]
    fmtcol = fmtcol.lower()
    return fmtcol

def csv_schema_reader(file_path,filename,table_name=None,db=None):

    # table_name = string_formatter(table_name)

    fileCsv = pd.read_csv(file_path+'/'+filename, nrows=100, error_bad_lines=False)
    columns = fileCsv.dtypes

    C = []
    D = []
    E = []
    for i in range(columns.size):
        if columns[i] == 'object':
            C.append(i)

        elif columns[i] == 'float64':
            D.append(i)

    if D != []:
        for i in D:
            data = fileCsv.iloc[:, i].tolist()
            a = type(data[0])
            b = [k for k, x in enumerate(data) if not np.isnan(x)]
            if b == []:
                E.append(i)


    fileCsv = pd.read_csv(file_path+'/'+filename, parse_dates=C, infer_datetime_format=False, nrows=200, error_bad_lines=False)
    if E != []:
        for i in E:
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].astype(str)
    # print data.dtypes
    data_types = fileCsv.dtypes
    columnsDetails = data_types.to_frame(name='dataType')
    columnsDetails['headderName'] = fileCsv.columns.values.tolist()

    schema = []
    for i in range(len(columnsDetails.index)):
        field_detail = {'name': string_formatter(columnsDetails.iloc[i]['headderName'],i),
                        'type': columnsDetails.iloc[i]['dataType']}
        schema.append(field_detail)
    print schema

    print "Field type recognition successful"
    bq_schema = []
    if db.lower() == 'bigquery':

        # schema_dict = {}
        j = 0
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
                fileCsv.iloc[:, j] = pd.to_datetime(fileCsv.iloc[:, j])
                fileCsv.iloc[:, j] = fileCsv.iloc[:, j].apply(lambda x: x.strftime('%H:%M:%S') if not pd.isnull(x) else None)
                fileCsv.iloc[:, j] = fileCsv.iloc[:, j].apply(lambda v: str(v))
                data=fileCsv.iloc[:, j].tolist()
                b = [k for k,x in enumerate(data) if x!='00:00:00']
                if b == []:
                    schema_dict['name'] = i['name']
                    schema_dict['type'] = 'DATE'
                    schema_dict['mode'] = 'nullable'
                else:
                    schema_dict['name'] = i['name']
                    schema_dict['type'] = 'DATETIME'
                    schema_dict['mode'] = 'nullable'
            j+=1
            bq_schema.append(schema_dict)

    try:
        with open(file_path + '/schema.txt', 'w') as outfile:
            json.dump(bq_schema, outfile)
    except Exception, err:
        print err

    return bq_schema