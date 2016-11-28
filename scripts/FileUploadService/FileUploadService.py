# -*- coding: utf-8 -*-
__author__ = 'Marlon Abeykoon'
__version__ = '2.0.0.0'

import xlrd
from xlsxwriter.workbook import Workbook
import csv
import glob
import zipfile
from wand import image as img
import datetime
from multiprocessing import Process
import FileDatabaseInsertion
import configs.ConfigHandler as conf
import sys,os
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
import modules.CommonMessageGenerator as cmg
import FileDatabaseInsertionCSV
import GetTableSchema
import json
from xml.dom import minidom
import pandas as pd
import re


def file_upload(params, file_obj,data_set_name, user_id, domain):
    start_time = datetime.datetime.now()
    print "File received.. Uploading started.."
    o_data = params.other_data

    if o_data == 'logo':
        upload_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                          'Path'] + '/digin_user_data/' + user_id + '/' + domain + '/logos'
        try:
            os.makedirs(upload_path)
        except OSError:
            if not os.path.isdir(upload_path):
                raise
        if 'file' in file_obj:  # to check if the file-object is created
            try:
                filename = file_obj.file.filename.replace('\\',
                                                          '/')  # replaces the windows-style slashes with linux ones.
                # filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
                fout = open(upload_path + '/' + filename,
                            'wb')  # creates the file where the uploaded file should be stored
                fout.write(file_obj.file.file.read())  # writes the uploaded file to the newly created file.
                fout.close()  # closes the file, upload complete.
            except Exception, err:
                print err
                return cmg.format_response(False, err, "Error occurred while uploading file", sys.exc_info())

            uploaded_time = datetime.datetime.now()
            time_taken = uploaded_time - start_time
            print "Upload completed! Time taken - " + str(time_taken)
            return cmg.format_response(True, 1, "File Upload successful!")

    elif o_data == 'dp':
        upload_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                          'Path'] + '/digin_user_data/' + user_id + '/' + domain + '/DPs'
        try:
            os.makedirs(upload_path)
        except OSError:
            if not os.path.isdir(upload_path):
                raise
        if 'file' in file_obj:  # to check if the file-object is created
            try:
                filename = file_obj.file.filename.replace('\\',
                                                          '/')  # replaces the windows-style slashes with linux ones.
                # filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
                fout = open(upload_path + '/' + filename,
                            'wb')  # creates the file where the uploaded file should be stored
                fout.write(file_obj.file.file.read())  # writes the uploaded file to the newly created file.
                fout.close()  # closes the file, upload complete.
            except Exception, err:
                return cmg.format_response(False, err, "Error occurred while uploading file", sys.exc_info())

            uploaded_time = datetime.datetime.now()
            time_taken = uploaded_time - start_time
            print "Upload completed! Time taken - " + str(time_taken)
            return cmg.format_response(True, 1, "File Upload successful!")

    elif o_data == 'widget_image':
        base_upload_path = conf.get_conf('FilePathConfig.ini', 'User Files')['Path']
        upload_path = base_upload_path + '/digin_user_data/' + user_id + '/' + domain + '/shared_files'
        document_root = conf.get_conf('FilePathConfig.ini', 'Document Root')['Path']
        path = re.sub(document_root, '', base_upload_path)
        try:
            os.makedirs(upload_path)
        except OSError:
            if not os.path.isdir(upload_path):
                raise
        if 'file' in file_obj:
            try:
                filename = file_obj.file.filename.replace('\\', '/')
                fout = open(upload_path + '/' + filename, 'wb')
                fout.write(file_obj.file.file.read())
                fout.close()
                _prepare_file('svg', upload_path, filename)
            except Exception, err:
                return cmg.format_response(False, err, "Error occurred while uploading file", sys.exc_info())
            return cmg.format_response(True, path, "File Upload successful!")
        return cmg.format_response(False, None, "Error occurred!", sys.exc_info())

    elif o_data == 'prpt_reports':
        # upload_path = conf.get_conf('FilePathConfig.ini','Reports')['Path']
        upload_path = conf.get_conf('FilePathConfig.ini', 'User Files')['Path'] + '/digin_user_data/' + user_id \
                      + '/' + domain + '/prpt_files'
        try:
            os.makedirs(upload_path)
        except OSError:
            if not os.path.isdir(upload_path):
                raise
        if 'file' in file_obj:
            try:
                filename = file_obj.file.filename.replace('\\', '/')
                fout = open(upload_path + '/' + filename, 'wb')
                fout.write(file_obj.file.file.read())
                fout.close()
            except Exception, err:
                return cmg.format_response(False, err, "Error occured while uploading file", sys.exc_info())
            extension = filename.split('.')[-1]
            _prepare_file(extension, upload_path, filename)
            _ktrConfig(user_id, domain, filename)

            return cmg.format_response(True, 1, "File Upload successful!")

    try:
        datasource_o_date = json.loads(o_data)['file_type']
    except Exception:
        datasource_o_date = ''

    if datasource_o_date == 'datasource':
        folder_name = json.loads(o_data)['folder_name']
        if folder_name is not None:
            if params.datasource_id is not None:
                upload_details = FileDatabaseInsertionCSV.get_data_source_details(params.datasource_id)
                user_id = upload_details['created_user']
                domain = upload_details['created_tenant']
            upload_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                              'Path'] + '/digin_user_data/' + user_id + '/' + domain + '/data_sources/' + folder_name
            try:
                os.makedirs(upload_path)
            except OSError:
                if not os.path.isdir(upload_path):
                    raise
        else:
            upload_path = conf.get_conf('FilePathConfig.ini', 'User Files')[
                              'Path'] + '/digin_user_data/' + user_id + '/' + domain + '/data_sources/'
            try:
                os.makedirs(upload_path)
            except OSError:
                if not os.path.isdir(upload_path):
                    raise

        print start_time
        filepath = upload_path
        if 'file' in file_obj:
            try:
                filename = file_obj.file.filename.replace('\\', '/')
                _extension = filename.split('.')[-1]

                if _extension.lower() =='csv':
                    fout = open(filepath + '/' + filename, 'wb')
                    fout.write(file_obj.file.file.read().decode(encoding='UTF-8',errors='ignore'))
                    fout.close()

                elif _extension.lower() =='xlsx'or _extension.lower() =='xlsx':
                    fout = open(filepath + '/' + filename, 'wb')
                    fout.write(file_obj.file.file.read())
                    fout.close()
            except Exception, err:
                return cmg.format_response(False, err, "Error occured while uploading file", sys.exc_info())

            uploaded_time = datetime.datetime.now()
            time_taken = uploaded_time - start_time
            print "Upload completed! Time taken - " + str(time_taken)
            extension = filename.split('.')[-1]
            print extension
            # p = Process(target=_prepare_file,args=(extension,filepath,filename,params,data_set_name,user_id,domain))
            # p.start()
            # return  cmg.format_response(True,1,"File Upload successful!")
            if folder_name is not None:
                try:
                    with open(filepath + '/schema.txt') as json_data:
                        schema = json.load(json_data)
                except Exception, err:
                    schema = _prepare_file(extension, filepath, filename, params, data_set_name, folder_name)

            else:
                schema = _prepare_file(extension, filepath, filename, params, data_set_name, folder_name)

            return cmg.format_response(True, schema, 'done')

    else:
        return cmg.format_response(False, None, "Error  occurred due to other_data parameter", sys.exc_info())


def _convert_to_xl(file_path, filename):
    for csvfile in glob.glob(file_path + '/' + filename):
        workbook = Workbook(csvfile + '.xlsx')
        worksheet = workbook.add_worksheet()
        try:
            with open(csvfile, 'rb') as f:
                reader = csv.reader(f)
                for r, row in enumerate(reader):
                    for c, col in enumerate(row):
                        worksheet.write(r, c, col)
        except Exception, err:
            print err
            print "Opening CSV in 'rb' mode failed trying to open in 'rU' mode(universal newline mode)!"
            with open(csvfile, 'rU') as f:
                reader = csv.reader(f)
                for r, row in enumerate(reader):
                    for c, col in enumerate(row):
                        worksheet.write(r, c, col)
        workbook.close()
    return


def _prepare_file(extension, file_path, filename, params=None, data_set_name=None, foldername=None, user_id=None,
                  tenant=None):
    print "File processing started!"
    if extension == 'xlsx' or extension == 'xls':
        # Open the workbook
        print 'Excel processing started...'
        print file_path
        file_name = filename.split('.')[0]
        xl_workbook = xlrd.open_workbook(file_path + '/' + filename)
        ws = xl_workbook.sheet_by_index(0)
        csv_file = open(file_path + '/' + file_name+'.csv', 'wb')
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        for rownum in xrange(ws.nrows):
            wr.writerow(ws.row_values(rownum))
        csv_file.close()
        # data_list = []
        # for rownum in range(ws.nrows):
        #     data_list += [ws.row_values(rownum)]
        # output = FileDatabaseInsertion.excel_uploader(xl_workbook, filename.split('.')[0], params.db, data_set_name,
        #                                               user_id, tenant)
        # csv_file = pd.read_csv(file_path + '/' + filename)
        # csv_file.to_csv(file_path + '/' + file_name+'.csv', index=False)
        output = GetTableSchema.csv_schema_reader(file_path, file_name+'.csv', foldername, params.db)
        return output


    elif extension == 'csv':
        print 'csv processing started...'
        # try:
        #     _convert_to_xl(file_path,filename)
        # except Exception, err:
        #     print err
        #     raise
        # xl_workbook = xlrd.open_workbook(file_path+'/'+filename+'.xlsx')
        # output = FileDatabaseInsertionCSV.csv_uploader(file_path,filename,filename.split('.')[0],params.db,data_set_name,user_id,tenant)
        output = GetTableSchema.csv_schema_reader(file_path, filename, foldername, params.db)
        return output

    elif extension == 'zip':
        fh = open(file_path + '/' + filename, 'rb')
        z = zipfile.ZipFile(fh)
        for name in z.namelist():
            outpath = file_path
            z.extract(name, outpath)
        fh.close()
        print 'zip successfully extracted!'
        return True

    elif extension == 'svg':
        svg_file = open(file_path + '/' + filename, "r")
        with img.Image(blob=svg_file.read(), format="svg") as image:
            png_image = image.make_blob("png")

        with open(file_path + '/' + filename[:-4] + '.png', "wb") as out:
            out.write(png_image)

    else:
        print "Extension not supported"
        raise


def _ktrConfig(user_id, domain, Filename):
    User_Reports_path = conf.get_conf('FilePathConfig.ini', 'User Files')['Path']
    filename = Filename.split('.')[0]
    try:

        xmldoc = minidom.parse(User_Reports_path + '/digin_user_data/' + user_id + '/' + domain + '/prpt_files/' + filename + '/' + filename + '.ktr')

        ReportDF = xmldoc.getElementsByTagName("item")[0]
        ReportDF.firstChild.nodeValue = User_Reports_path + '/digin_user_data/' + user_id + '/' + domain + '/prpt_files/' + filename + '/' + filename + '.prpt'
        OutPut = xmldoc.getElementsByTagName("item")[1]
        OutPut.firstChild.nodeValue = User_Reports_path + '/digin_user_data/' + user_id + '/' + domain + '/prpt_files/' + filename + '/' + filename + '.pdf'

        with open(
            User_Reports_path + '/digin_user_data/' + user_id + '/' + domain + '/prpt_files/' + filename + '/' + filename + '.ktr',"wb") as f:
            xmldoc.writexml(f)

    except Exception, err:
        print err
        print "No such Ktr file "