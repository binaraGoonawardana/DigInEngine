__author__ = 'Marlon Abeykoon'

import xlrd
import csv
import datetime
from multiprocessing import Process
import FileDatabaseInsertion
from time import gmtime, strftime
import configs.ConfigHandler as conf
import sys,os
currDir = os.path.dirname(os.path.realpath(__file__))
print currDir
rootDir = os.path.abspath(os.path.join(currDir, '../..'))
if rootDir not in sys.path:  # add parent dir to paths
    sys.path.append(rootDir)
print rootDir
import modules.CommonMessageGenerator as cmg

upload_path = conf.get_conf('FilePathConfig.ini','Uploads')['Path']

def file_upload(params, file_obj):
        #start_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        start_time = datetime.datetime.now()
        print start_time
        #x = params.input(file={})
        filedir = upload_path # change this to the directory you want to store the file in.
        if 'file' in file_obj: # to check if the file-object is created
            try:
                filepath=file_obj.file.filename.replace('\\','/') # replaces the windows-style slashes with linux ones.
                filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
                fout = open(filedir +'/'+ filename,'wb') # creates the file where the uploaded file should be stored
                fout.write(file_obj.file.file.read()) # writes the uploaded file to the newly created file.
                fout.close() # closes the file, upload complete.
            except Exception, err:
                return  cmg.format_response(False,None,"Error occured while uploading file",sys.exc_info())

            #uploaded_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            uploaded_time = datetime.datetime.now()
            time_taken = uploaded_time - start_time
            print "Upload completed! Time taken - " + str(time_taken)
            extension = filename.split('.')[-1]
            print extension
            p = Process(target=insertion_preparation,args=(extension,filedir,filepath,filename,params))
            p.start()
            return  cmg.format_response(True,1,"File Upload successful!")

def insertion_preparation(extension,filedir,filepath,filename,params):
            print "File processing started!"
            if extension == 'xlsx' or extension =='xls':
                # Open the workbook
                print 'Excel processing started...'
                print filedir+'/'+filepath
                xl_workbook = xlrd.open_workbook(filedir+'/'+filepath)
                ws = xl_workbook.sheet_by_index(0)
                data_list = []
                for rownum in range(ws.nrows):
                    data_list += [ws.row_values(rownum)]
                output = FileDatabaseInsertion.sql(filedir+'/'+filepath,filename.split('.')[0],params.db,data_list)
                return output


            elif extension == 'csv':
                print 'csv processing started...'
                with open(filedir+'/'+filepath) as f:
                    reader = csv.reader(f)
                    data_list = list(reader)
                    output = FileDatabaseInsertion.sql(filedir+'/'+filepath,filename.split('.')[0],params.db,data_list)
                    print output
            #else: raise UnsupportedFormat("Uploaded file type not supported!")
                   #else: output = "Error occurred while uploading!"
        #print strftime("%Y-%m-%d %H:%M:%S", gmtime())
        #return output

