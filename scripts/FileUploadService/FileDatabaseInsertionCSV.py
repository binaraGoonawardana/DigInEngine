# -*- coding: utf-8 -*-
__author__ = 'Jeganathan Thivatharan'
__version__ = '3.0.0.0.6'

import pandas as pd
import modules.BigQueryHandler as bq
import modules.PostgresHandler as pg
import configs.ConfigHandler as conf
import string
import json
import sys
import re
from datetime import datetime
import scripts.utils.DiginIDGenerator as idgun
import modules.CommonMessageGenerator as comm
import scripts.DigINCacheEngine.CacheController as db



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
            # fileCsv.iloc[:, i] = fileCsv.iloc[:, i].map(lambda x: re.sub('[\n\\r\n]', '', str(x)))
            # fileCsv.iloc[:, i] = fileCsv.iloc[:, i].map(lambda x: ''.join(str(x).splitlines()))
            try:
                fileCsv.iloc[:, i] = fileCsv.iloc[:, i].str.replace('[\n\\r\n]', ' ')
            except:
                fileCsv.iloc[:, i] = fileCsv.iloc[:, i]
            #fileCsv.iloc[:, i] = fileCsv.iloc[:, i].astype(str)
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

        elif column['type'].lower() == 'datetime':
            fileCsv.iloc[:, i] = pd.to_datetime(fileCsv.iloc[:, i])
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].apply(lambda v: str(v))

        # elif column['type'].lower() == 'integer':
        #     fileCsv.iloc[:,i] = fileCsv.iloc[:,i].astype(int)
            # t = threading.Thread(target=_to_integer, args=(i,fileCsv.iloc[:,i], _list))
            # t.start()
            # threads.append(t)

        elif column['type'].lower() == 'date':
            fileCsv.iloc[:, i] = pd.to_datetime(fileCsv.iloc[:, i])
            fileCsv.iloc[:, i] = fileCsv.iloc[:,i].apply(lambda x: x.strftime('%Y-%m-%d'))
            fileCsv.iloc[:, i] = fileCsv.iloc[:, i].apply(lambda v: str(v))
             # fileCsv.iloc[:, i] = fileCsv.iloc[:,i].apply(lambda x: x.strftime('%d%m%Y'))


        elif column['type'].lower() == 'time':
            fileCsv.iloc[:, i] = pd.to_datetime(fileCsv.iloc[:, i])
            fileCsv.iloc[:, i] = fileCsv.iloc[:,i].apply(lambda x: x.strftime('%H:%M:%S'))
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
    data_source_id = 0
    first_row_number = 1
    folder_name = parms.folder_name
    filename = parms.filename
    extension = filename.split('.')[-1]
    if extension == 'xlsx' or extension == 'xls':
        filename = filename.split('.')[0]+'.csv'

    file_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                    'Path'] + '/digin_user_data/' + user_id + '/' + tenant + '/data_sources/' + folder_name

    schema = {}
    if parms.folder_type.lower() == 'new':
        datasource_type = 'csv-directory'
        try:
            schema = json.loads(parms.schema)
            with open(file_path + '/schema.txt', 'w') as outfile:
                json.dump(schema, outfile)
        except Exception, err:
            print err
    elif parms.folder_type.lower() == 'singlefile':
        datasource_type = 'csv-singlefile'
        try:
            folder_name = filename.split('.')[0]
            schema = json.loads(parms.schema)
            with open(file_path + folder_name+'.txt', 'w') as outfile:
                json.dump(schema, outfile)
        except Exception, err:
            print err
    elif parms.folder_type.lower() == 'exist':
        try:
            data_source_details = get_data_source_details(parms.datasource_id)
        except Exception, err:
            print err
            print "Error occurred in table creation!"
            return err

        data_source_id = int(parms.datasource_id)
        created_user = data_source_details['created_user']
        created_tenant = data_source_details['created_tenant']
        dataset_name = data_source_details['dataset_id']
        schema = data_source_details['schema']
        datasource_type = 'csv-directory'
        try:
            first_row_number = _get_upload_details(parms.datasource_id)
        except Exception, err:
            print err
            return err

        file_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                    'Path'] + '/digin_user_data/' + created_user + '/' + created_tenant + '/data_sources/' + folder_name
        try:
            with open(file_path + '/schema.txt') as json_data:
                schema = json.load(json_data)
        except Exception, err:
            print err

    db = parms.db
    table_name = string_formatter(folder_name)
    if parms.is_first_try == False or parms.is_first_try == 'False':
        schema.insert(0, {"type": "integer", "name": "_index_id", "mode": "nullable"})
    try:
        file_csv = pd.read_csv(file_path+'/'+filename,error_bad_lines=False)
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
        file_csv = pd.read_csv(file_path+'/'+filename, date_parser=C, error_bad_lines=False, dayfirst=True)
    except Exception,err:
        print err
        result = comm.format_response(False,err,"failed read csv file",exception=sys.exc_info())
        return result
    print "Field type recognition successful"

    if db.lower() == 'bigquery':

        print "Data casting started!"
        try:
            _list = _cast_data(schema, file_csv)
        except Exception, err:
            print err
            result = comm.format_response(False, err, "Error occurred while DataCasting.. \n" + str(err), exception=sys.exc_info())
            if parms.folder_type.lower() == 'new' or parms.folder_type.lower() == 'singlefile':
                table_delete_status = bq.delete_table(dataset_name,table_name)
                print 'Table delete status: ' + str(table_delete_status)
            return result
        print 'Data casting successful'

        try:
            if parms.is_first_try == True or parms.is_first_try == 'True':
                schema.insert(0, {"type": "integer", "name": "_index_id", "mode": "nullable"})

            table_existance = bq.check_table(dataset_name,table_name)
            security_level = 'write'
            if table_existance :
                if parms.folder_type.lower() == 'singlefile':
                    bq.delete_table(dataset_name,table_name)
                    print "Existing Table deleted"
                    try:
                        print dataset_name
                        result = bq.create_Table(dataset_name, table_name, schema, security_level, user_id, tenant)
                        if result:
                            print "Table creation succcessful!"
                        else:
                            print "Error occurred while creating table! If table already exists data might insert to the existing table!"

                    except Exception, err:
                        print "Error occurred while creating table!"
                        print err
                        raise
            else:
                try:
                    print dataset_name
                    Result = bq.create_Table(dataset_name,table_name,schema, security_level, user_id, tenant)
                    if Result:
                        data_source_id = Result
                        print "Table creation succcessful!"
                    else: print "Error occurred while creating table! If table already exists data might insert to the existing table!"

                except Exception, err:
                    print "Error occurred while creating table!"
                    print err
                    raise

        except Exception, err:
            print err

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
        if parms.is_first_try == True or parms.is_first_try == 'True':
            number_rows = len(_list.index)
            _list.index = range(first_row_number, first_row_number + number_rows, 1)
            _list.to_csv(file_path+'/'+filename,header=None)
        else:
            number_rows = len(_list.index)
            _list.to_csv(file_path + '/' + filename, index=False)

        try:
            result = bq.inser_data(schema,dataset_name,table_name,file_path,filename,user_id,tenant)

            try:
                _data_insertion_to_upload_details(data_source_id,user_id,number_rows,first_row_number,datasource_type,filename)
            except Exception, err:
                print "Error inserting to cacheDB!"
                return comm.format_response(False, err, "Error occurred while inserting.. \n" + str(err),
                                                        exception=sys.exc_info())
        except Exception, err:
            try:
                err1 = str(err)
                err1 = re.sub("Too many errors encountered.\n", '', err1)
                words = err1.split()
                if words[0] == '<HttpError' and words[1] == '400':
                    err1="Empty schema specified for the load job. Please specify a schema that describes the data being loaded."
                err1 = re.sub('<', '', err1)
                err1 = re.sub('>', '', err1)
            except Exception:
                err1 = str(err)
            print err1
            result = comm.format_response(False, err1, "Error occurred while inserting.. \n"+ str(err1), exception=None)
            if parms.folder_type.lower() == 'new' or parms.folder_type.lower() == 'singlefile':
                table_delete_status = bq.delete_table(dataset_name,table_name)
                print 'Table delete status: ' + str(table_delete_status)
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
    return True


def _data_insertion_to_upload_details(datasource_id, upload_user, number_of_rows, first_row_number, datasource_type, file_name):
    data = {
        'upload_id': idgun.unix_time_millis_id(datetime.now()),
        'datasource_id': datasource_id,
        'uploaded_datetime': datetime.now(),
        'modified_datetime': datetime.now(),
        'upload_type': datasource_type,
        'file_name': file_name,
        'upload_user': upload_user,
        'number_of_rows': number_of_rows,
        'first_row_number': first_row_number,
        'is_deleted': False
    }
    db.insert_data([data], 'digin_datasource_upload_details')

def _get_upload_details(data_source_id):

    query = db.get_data('SELECT  number_of_rows,first_row_number FROM digin_datasource_upload_details '\
            'where  datasource_id = {0} '\
            'order by upload_id DESC limit 1 '.format(int(data_source_id)))['rows']
    row_no = query[0][0] + query[0][1]
    return row_no

def get_data_source_details(data_source_id):
    query = db.get_data('SELECT * FROM digin_datasource_details '\
                        'where id = {0}'.format(int(data_source_id)))['rows']

    table_details = {
        'dataset_id': query[0][2],
        'datasource_id': query[0][3],
        'schema': query[0][5],
        'created_user': query[0][8],
        'created_tenant':query[0][9]
    }
    return table_details