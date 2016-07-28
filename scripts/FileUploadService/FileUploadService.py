# -*- coding: utf-8 -*-
__author__ = 'Marlon Abeykoon'

import xlrd
from xlsxwriter.workbook import Workbook
import csv
import glob
import zipfile
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



def file_upload(params, file_obj,data_set_name, user_id, domain):

        start_time = datetime.datetime.now()
        print "File received.. Uploading started.."
        o_data = params.other_data

        if o_data == 'logo':
            upload_path = conf.get_conf('FilePathConfig.ini','User Files')['Path']+'/digin_user_data/'+user_id+'/'+domain+'/logos'
            try:
                os.makedirs(upload_path)
            except OSError:
                if not os.path.isdir(upload_path):
                    raise
            if 'file' in file_obj: # to check if the file-object is created
                try:
                    filename=file_obj.file.filename.replace('\\','/') # replaces the windows-style slashes with linux ones.
                    #filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
                    fout = open(upload_path +'/'+ filename,'wb') # creates the file where the uploaded file should be stored
                    fout.write(file_obj.file.file.read()) # writes the uploaded file to the newly created file.
                    fout.close() # closes the file, upload complete.
                except Exception, err:
                    print err
                    return  cmg.format_response(False,err,"Error occurred while uploading file",sys.exc_info())

                uploaded_time = datetime.datetime.now()
                time_taken = uploaded_time - start_time
                print "Upload completed! Time taken - " + str(time_taken)
                return  cmg.format_response(True,1,"File Upload successful!")

        if o_data == 'dp':
            upload_path = conf.get_conf('FilePathConfig.ini','User Files')['Path']+'/digin_user_data/'+user_id+'/'+domain+'/DPs'
            try:
                os.makedirs(upload_path)
            except OSError:
                if not os.path.isdir(upload_path):
                    raise
            if 'file' in file_obj: # to check if the file-object is created
                try:
                    filename=file_obj.file.filename.replace('\\','/') # replaces the windows-style slashes with linux ones.
                    #filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
                    fout = open(upload_path +'/'+ filename,'wb') # creates the file where the uploaded file should be stored
                    fout.write(file_obj.file.file.read()) # writes the uploaded file to the newly created file.
                    fout.close() # closes the file, upload complete.
                except Exception, err:
                    return  cmg.format_response(False,err,"Error occurred while uploading file",sys.exc_info())

                uploaded_time = datetime.datetime.now()
                time_taken = uploaded_time - start_time
                print "Upload completed! Time taken - " + str(time_taken)
                return  cmg.format_response(True,1,"File Upload successful!")

        elif o_data == 'datasource':
            upload_path = conf.get_conf('FilePathConfig.ini','User Files')['Path']+'/digin_user_data/'+user_id+'/'+domain+'/data_sources'
            try:
                os.makedirs(upload_path)
            except OSError:
                if not os.path.isdir(upload_path):
                    raise

            print start_time
            filepath = upload_path
            if 'file' in file_obj:
                try:
                    filename=file_obj.file.filename.replace('\\','/')
                    fout = open(filepath +'/'+ filename,'wb')
                    fout.write(file_obj.file.file.read())
                    fout.close()
                except Exception, err:
                    return  cmg.format_response(False,err,"Error occured while uploading file",sys.exc_info())

                uploaded_time = datetime.datetime.now()
                time_taken = uploaded_time - start_time
                print "Upload completed! Time taken - " + str(time_taken)
                extension = filename.split('.')[-1]
                print extension
                p = Process(target=_prepare_file,args=(extension,filepath,filename,params,data_set_name))
                p.start()
                return  cmg.format_response(True,1,"File Upload successful!")

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
                        filename=file_obj.file.filename.replace('\\','/')
                        fout = open(upload_path +'/'+ filename,'wb')
                        fout.write(file_obj.file.file.read())
                        fout.close()
                    except Exception, err:
                        return  cmg.format_response(False,err,"Error occured while uploading file",sys.exc_info())
                    extension = filename.split('.')[-1]
                    _prepare_file(extension,upload_path,filename)
                    return cmg.format_response(True,1,"File Upload successful!")
        else:
            return  cmg.format_response(False,None,"Error  occurred due to other_data parameter",sys.exc_info())


def _convert_to_xl(file_path,filename):
    for csvfile in glob.glob(file_path+'/'+filename):
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

def _prepare_file(extension,file_path,filename,params=None,data_set_name=None):
            print "File processing started!"
            if extension == 'xlsx' or extension =='xls':
                # Open the workbook
                print 'Excel processing started...'
                print file_path
                xl_workbook = xlrd.open_workbook(file_path+'/'+filename)
                #ws = xl_workbook.sheet_by_index(0)
                # data_list = []
                # for rownum in range(ws.nrows):
                #     data_list += [ws.row_values(rownum)]
                output = FileDatabaseInsertion.excel_uploader(xl_workbook,filename.split('.')[0],params.db,data_set_name)
                return output


            elif extension == 'csv':
                print 'csv processing started...'
                try:
                    _convert_to_xl(file_path,filename)
                except Exception, err:
                    print err
                    raise
                xl_workbook = xlrd.open_workbook(file_path+'/'+filename+'.xlsx')
                output = FileDatabaseInsertion.excel_uploader(xl_workbook,filename.split('.')[0],params.db,data_set_name)
                print output

            elif extension == 'zip':
                fh = open(file_path+'/'+filename, 'rb')
                z = zipfile.ZipFile(fh)
                for name in z.namelist():
                    outpath = file_path
                    z.extract(name, outpath)
                fh.close()
                print 'zip successfully extracted!'
                return True

            else:
                print "Extension not supported"
                raise
