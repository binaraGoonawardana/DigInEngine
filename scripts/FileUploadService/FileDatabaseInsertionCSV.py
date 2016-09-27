# -*- coding: utf-8 -*-
__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.1'

import pandas as pd
import modules.BigQueryHandler as bq
import modules.PostgresHandler as pg
import configs.ConfigHandler as conf
import string
import threading
import json
from datetime import datetime
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

def _table_creation_bq(_schema, db, data_set_name, table_name):
    if db.lower() == 'bigquery':
        bq_schema = []
        # schema_dict = {}
        for i in _schema:
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

        print 'Table creation started!'
        print table_name
        try:
            print data_set_name
            result = bq.create_Table(data_set_name,table_name,bq_schema)
            if result:
                print "Table creation succcessful!"
            else: print "Error occurred while creating table! If table already exists data might insert to the existing table!"
        except Exception, err:
            print "Error occurred while creating table!"
            print err
            raise

def _float_or_zero(value):
    try:
        return float(value)
    except:
        return 0.0

def _to_string(index, data_list, column_list):
    column_list.append({'index': index, 'col_data':  map(str, data_list)})

def _to_float(index, data_list, column_list):
    column_list.append({'index': index, 'col_data' : map(_float_or_zero, data_list)})

def _to_date(index, data_list, column_list):
    casted_date_list = []
    for dates in data_list:
        if type(dates) == pd.tslib.NaTType:
            dates = None
            casted_date_list.append(dates)
        else:
            casted_date_list.append(str(dates))

    column_list.append({'index': index, 'col_data': casted_date_list})

def _to_integer(index, data_list, column_list):
    column_list.append({'index': index, 'col_data': map(int, data_list)})


def _cast_data(schema, fileCsv):
    i = 0
    _list = []
    threads = []
    for column in schema:

        if column['type'] == 'string':
            t = threading.Thread(target=_to_string, args=(i,fileCsv.iloc[:,i], _list))
            t.start()
            threads.append(t)

        elif column['type'] == 'float':
            t = threading.Thread(target=_to_float, args=(i,fileCsv.iloc[:,i], _list))
            t.start()
            threads.append(t)

        elif column['type']  == 'TIMESTAMP':
            t = threading.Thread(target=_to_date, args=(i,fileCsv.iloc[:,i], _list))
            t.start()
            threads.append(t)

        elif column['type']  == 'integer':
            t = threading.Thread(target=_to_integer, args=(i,fileCsv.iloc[:,i], _list))
            t.start()
            threads.append(t)

        i += 1
        print "Currently casting column number: " + str(i)


    for t in threads:
        t.join()

    return  _list

def _data_insertion(data_set_name,table_name,data,user_id=None,tenant=None):
    print 'Data insertion started!'
    try:
        result = bq.Insert_Data(data_set_name,table_name,data,user_id,tenant)
        print result
        print "Data insertion successful!"
    except Exception, err:
        print "Error occurred while inserting data!"
        print err
        raise
    return result


def csv_uploader(parms, dataset_name, user_id=None, tenant=None):
    folder_name = parms.folder_name
    filename = parms.filename
    schema = json.loads(parms.schema)
    db = parms.db
    file_path = conf.get_conf('FilePathConfig.ini', 'User Files')['Path'] + '/digin_user_data/' + user_id + '/' + tenant + '/data_sources/' + folder_name
    table_name = string_formatter(folder_name)

    fileCsv = pd.read_csv(file_path+'/'+filename)
    columns = fileCsv.dtypes

    C = []
    for i in range(columns.size):
        if columns[i] == 'object':
            C.append(i)

    fileCsv = pd.read_csv(file_path+'/'+filename, parse_dates=C, infer_datetime_format=True)
    # print data.dtypes
    # data_types = fileCsv.dtypes
    # columnsDetails = data_types.to_frame(name='dataType')
    # columnsDetails['headderName'] = fileCsv.columns.values.tolist()
    #
    # schema = []
    # for i in range(len(columnsDetails.index)):
    #     field_detail = {'name': string_formatter(columnsDetails.iloc[i]['headderName']),
    #                     'type': columnsDetails.iloc[i]['dataType']}
    #     schema.append(field_detail)
    # print schema

    print "Field type recognition successful"
    if db.lower() == 'bigquery':
        # table_creation_thread = threading.Thread(target=_table_creation_bq, args=(schema, db, dataset_name, table_name))
        # table_creation_thread.start()
        try:
            print dataset_name
            result = bq.create_Table(dataset_name,table_name,schema)
            if result:
                print "Table creation succcessful!"
            else: print "Error occurred while creating table! If table already exists data might insert to the existing table!"
        except Exception, err:
            print "Error occurred while creating table!"
            print err
            raise

        print "Data casting started!"
        _list = _cast_data(schema, fileCsv)
        print 'Data casting successful'
        _sorted_list = sorted(_list, key=lambda k: k['index'])

        data = []
        for row_x in range(len(fileCsv.index)):
            i = 0
            row = {}
            for col_y in schema:
                row[col_y['name']] = _sorted_list[i]['col_data'][row_x]
                i += 1
            data.append(row)

        # table_creation_thread.join()
        result = _data_insertion(dataset_name,table_name,data,user_id,tenant)
        print result
        print datetime.now()
        print 'Insertion Done!'
        return True

    elif db.lower() == 'postgresql':
        #table_creation_thread = threading.Thread(target=PostgresCreateTable, args=(schema, table_name))
        #table_creation_thread.start()
        CreateTable = PostgresCreateTable(schema, table_name)
        print 'CreateTable',CreateTable
        csv_insertion = pg.csv_insert(file_path+'/'+filename,table_name,True)
        print 'Insertion Done!',csv_insertion
        return True
def PostgresCreateTable(_schema, table_name):

    print "Table creation started!"
    sql = 'CREATE TABLE %s\n(' % (table_name)

    for i in _schema:

        t = i['type']
        print i['type']
        if t == 'object':
            #field_types[k] = 'character varying'
            sql = sql + '{0} character varying,'.format(i['name'])
        elif t == 'int64':
            #field_types[k] = 'integer'
            sql = sql + '{0} integer,'.format(i['name'])
        elif t == 'float64':
            #field_types[k] = 'NUMERIC'
            sql = sql + '{0} NUMERIC,'.format(i['name'] )

        elif t == 'datetime64[ns]':
            sql = sql + '{0} timestamp,'.format(i['name'])


    sql = sql[:len(sql ) -1] + '\n)'
    # sql = 'CREATE TABLE %s\n(' % (tblname)
    # for col in cols:
    #     sql = sql + ('\n\t{0} NVARCHAR({1}) NULL,'.format(col[0],col[1]))
    # sql = sql[:len(sql)-1] + '\n)'
    print sql
    try:
        result = pg.execute_query(sql)
        print result
        print "Table creation successful!"
    except Exception, err:
        print err
        print "Error occurred in table creation!"
