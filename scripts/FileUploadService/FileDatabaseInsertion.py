# -*- coding: utf-8 -*-
__author__ = 'Marlon Abeykoon'

import string
import sys
sys.path.append("...")
#import modules.PostgresHandler as pg
import modules.BigQueryHandler as bq
#import modules.SQLQueryHandler as mssql
import datetime
import xlrd
import threading
import logging
import configs.ConfigHandler as conf

path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/FileUpload.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')


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
            if t == 'string':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'string'
                schema_dict['mode'] = 'nullable'
            elif t == 'integer':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'integer'
                schema_dict['mode'] = 'nullable'
            elif t == 'float' or t == 'long':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'float'
                schema_dict['mode'] = 'nullable'
            elif t == 'date':
                schema_dict['name'] = i['name']
                schema_dict['type'] = 'date'
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

def _field_type_recognition(ncols, col_types, schema):
    for col_x in range(0,ncols):

        if 1 in col_types(col_x)[1:]:  # [1:] is to remove type of header
            schema[col_x]['type'] = 'string'
        elif set([2,3]).issubset(col_types(col_x)[1:]) or set([2,4]).issubset(col_types(col_x)[1:])\
                or set([3,4]).issubset(col_types(col_x)[1:]):
            schema[col_x]['type'] = 'string'
        elif 3 in col_types(col_x)[1:]:
            schema[col_x]['type'] = 'date'
        elif 4 in col_types(col_x)[1:]:
            schema[col_x]['type'] = 'boolean'
        elif 2 in col_types(col_x)[1:]:
            schema[col_x]['type'] = 'float'
        elif all(i == 6 for i in col_types(col_x)[1:]):
            schema[col_x]['type'] = 'string'
        else:
            schema[col_x]['type'] = 'string'

    return schema

def _float_or_zero(value):
    try:
        return float(value)
    except Exception:
        return 0.0

def _to_string(index, data_list, list):
    list.append({'index': index, 'col_data' : map(str, data_list)})

def _to_float(index, data_list, list):
    list.append({'index': index, 'col_data' : map(_float_or_zero, data_list)})

def _to_date(index, data_list, list, excel_obj):
    casted_date_list = []
    for date in data_list:
        try:
            casted_date = xlrd.xldate.xldate_as_datetime(date, excel_obj.datemode).strftime('%Y-%m-%d')
            casted_date_list.append(casted_date)
        except ValueError:
            casted_date = None
            casted_date_list.append(casted_date)
    list.append({'index': index, 'col_data' : casted_date_list})

def _cast_data(schema, col_values, excel_obj):
    i = 0
    _list = []
    threads = []
    for column in schema:

        if column['type'] == 'string':
            t = threading.Thread(target=_to_string, args=(i,col_values(i)[1:], _list))
            t.start()
            threads.append(t)

        elif column['type'] == 'float':
            t = threading.Thread(target=_to_float, args=(i,col_values(i)[1:], _list))
            t.start()
            threads.append(t)

        elif column['type']  == 'date':
            t = threading.Thread(target=_to_date, args=(i,col_values(i)[1:], _list,excel_obj))
            t.start()
            threads.append(t)

        i += 1
        print "Currently casting column number: " + str(i)


    for t in threads:
        t.join()

    return  _list

def excel_uploader(excel_obj, table_name, db, dataset_name, user_id=None, tenant=None):
    sh = excel_obj.sheet_by_index(0)
    print datetime.datetime.now()
    table_name = string_formatter(table_name)
    header = sh._cell_values[0]
    schema = []
    for field in header:
        field_detail = {'name':string_formatter(field),
                        'type':None}
        schema.append(field_detail)

    print "Field type recognition started.."

    schema = _field_type_recognition(sh.ncols, sh.col_types, schema)

    print schema
    print "Field type recognition successful"
    table_creation_thread = threading.Thread(target=_table_creation_bq, args=(schema, db, dataset_name, table_name))
    table_creation_thread.start()

    print "Data casting started!"
    _list = _cast_data(schema, sh.col_values, excel_obj)
    print 'Data casting successful'
    _sorted_list = sorted(_list, key=lambda k: k['index'])

    data = []
    for row_x in range(0,sh.nrows-1):
        i = 0
        row = {}
        for col_y in schema:
            row[col_y['name']] = _sorted_list[i]['col_data'][row_x]
            i += 1
        data.append(row)

    table_creation_thread.join()
    result = _data_insertion(dataset_name,table_name,data,user_id=user_id,tenant=tenant)
    print result
    print datetime.datetime.now()
    print 'Insertion Done!'
    return True