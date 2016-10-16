# -*- coding: utf-8 -*-
__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.1'

import pandas as pd
import modules.BigQueryHandler as bq
import modules.PostgresHandler as pg
import configs.ConfigHandler as conf
import string
import json
import sys
from datetime import datetime
import modules.CommonMessageGenerator as comm



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
    # _list = []
    # threads = []
    for column in schema:

        if column['type'].lower() == 'string':
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].astype(str)
            # t = threading.Thread(target=_to_string, args=(i,fileCsv.iloc[:,i], _list))
            # t.start()
            # threads.append(t)

        elif column['type'].lower() == 'float':
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].astype(float)
            # t = threading.Thread(target=_to_float, args=(i,fileCsv.iloc[:,i], _list))
            # t.start()
            # threads.append(t)

        elif column['type'].lower() == 'timestamp':
            fileCsv.iloc[:, i] = pd.to_datetime(fileCsv.iloc[:, i])
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].apply(lambda v: str(v))
            # t = threading.Thread(target=_to_date, args=(i,fileCsv.iloc[:,i], _list))
            # t.start()
            # threads.append(t)

        elif column['type'].lower() == 'integer':
            fileCsv.iloc[:,i] = fileCsv.iloc[:,i].astype(int)
            # t = threading.Thread(target=_to_integer, args=(i,fileCsv.iloc[:,i], _list))
            # t.start()
            # threads.append(t)

        elif column['type'].lower() == 'date':
            fileCsv.iloc[:, i] = pd.to_datetime(fileCsv.iloc[:, i], format='%Y-%m-%d')
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].apply(lambda v: str(v))
             # fileCsv.iloc[:, i] = fileCsv.iloc[:,i].apply(lambda x: x.strftime('%d%m%Y'))


        elif column['type'].lower() == 'time':
            fileCsv.iloc[:, i] = pd.to_datetime(fileCsv.iloc[:, i], format='%H:%M:%S')
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].apply(lambda v: str(v))
             # fileCsv.iloc[:, i] = fileCsv.iloc[:,i].apply(lambda x: x.strftime('%H%M%S'))


        i += 1
        print "Currently casting column number: " + str(i)


    # for t in threads:
    #     try:
    #         t.join()
    #     except Exception, err:
    #         print err
    #         result = comm.format_response(False, err, "Check the custom message", exception=None)
    #         return result
    return fileCsv

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
    file_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                    'Path'] + '/digin_user_data/' + user_id + '/' + tenant + '/data_sources/' + folder_name

    schema = {}
    if parms.folder_type.lower() == 'new':
        try:
            schema = json.loads(parms.schema)
            with open(file_path + '/schema.txt', 'w') as outfile:
                json.dump(schema, outfile)
        except Exception, err:
            print err
    elif parms.folder_type.lower() == 'singlefile':
        try:
            folder_name = filename.split('.')[0]
            schema = json.loads(parms.schema)
            with open(file_path + folder_name+'.txt', 'w') as outfile:
                json.dump(schema, outfile)
        except Exception, err:
            print err
    elif parms.folder_type.lower() == 'exist':
        try:
            with open(file_path + '/schema.txt') as json_data:
                schema = json.load(json_data)
        except Exception, err:
            print err

    db = parms.db
    table_name = string_formatter(folder_name)
    try:
        file_csv = pd.read_csv(file_path+'/'+filename)
    except Exception,err:
        print err
        result = comm.format_response(False,err,"Check the custom message",exception=sys.exc_info())
        return result

    columns = file_csv.dtypes
    C = []
    for i in range(columns.size):
        if columns[i] == 'object':
            C.append(i)

    try:
        file_csv = pd.read_csv(file_path+'/'+filename, date_parser=C)
    except Exception,err:
        print err
        result = comm.format_response(False,err,"failed read csv file",exception=sys.exc_info())
        return result
    print "Field type recognition successful"

    if db.lower() == 'bigquery':

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
        try:
            _list = _cast_data(schema, file_csv)
        except Exception, err:
            print err
            result = comm.format_response(False, err, "Error occurred while DataCasting..", exception=sys.exc_info())
            return result
        print 'Data casting successful'
        # try:
        #     _sorted_list = sorted(_list, key=lambda k: k['index'])
        # except Exception, err:
        #     print err
        #     result = comm.format_response(False, err, "Error occurred by configured schema and file data types do not match..", exception=None)
        #     return result
        #
        # data = []
        # for row_x in range(len(file_csv.index)):
        #     try:
        #         i = 0
        #         row = {}
        #         for col_y in schema:
        #             row[col_y['name']] = _sorted_list[i]['col_data'][row_x]
        #             i += 1
        #         data.append(row)
        #     except Exception, err:
        #         print err
        #         result = comm.format_response(False, err, "Configured schema and file data types do not match..", exception=sys.exc_info())
        #         return result
        #
        # keys = data[0].keys()
        # with open(file_path+'/'+filename, 'wb') as output_file:
        #     dict_writer = csv.DictWriter(output_file, keys)
        #     # dict_writer.writeheader()
        #     dict_writer.writerows(data)

        # file_csv = pd.DataFrame(data)
        _list.to_csv(file_path+'/'+filename,index=False, header=None)

        try:
            result = bq.inser_data(schema,dataset_name,table_name,file_path,filename)
        except Exception, err:
            print err
            result = comm.format_response(False, err, "Error occurred while inserting..", exception=sys.exc_info())
            return result

        print result
        print datetime.now()
        print 'Insertion Done!'
        result = comm.format_response(True, result, "Successfully inserted ", exception=None)
        print result
        return result

    elif db.lower() == 'postgresql':
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
