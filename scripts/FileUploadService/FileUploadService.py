__author__ = 'Marlon Abeykoon'

import xlrd
import FileDatabaseInsertion
from time import gmtime, strftime
import configs.ConfigHandler as conf

upload_path = conf.get_conf('FilePathConfig.ini','Uploads')['Path']

def file_upload(params, file_obj):
        print strftime("%Y-%m-%d %H:%M:%S", gmtime())
        #x = params.input(file={})
        filedir = upload_path # change this to the directory you want to store the file in.
        if 'file' in file_obj: # to check if the file-object is created
            x = file_obj
            #contents = unicode(x['file'].value, 'cp866')
            #print contents
            # web.debug(x['myfile'].value)
            filepath=file_obj.file.filename.replace('\\','/') # replaces the windows-style slashes with linux ones.
            filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
            fout = open(filedir +'/'+ filename,'wb') # creates the file where the uploaded file should be stored
            fout.write(file_obj.file.file.read()) # writes the uploaded file to the newly created file.
            fout.close() # closes the file, upload complete.
            extension = filename.split('.')[-1]
            print extension
            if extension == 'xlsx':
                # Open the workbook
                print filedir+'/'+filepath
                xl_workbook = xlrd.open_workbook(filedir+'/'+filepath)
                #xl_sheet = xl_workbook.sheet_by_index(0)
                #row = xl_sheet.row(0)

                ws = xl_workbook.sheet_by_index(0)
                for rownum in xrange(ws.nrows):
                    y = tuple(ws.row_values(rownum))
                    print y
                from xlrd.sheet import ctype_text

            elif extension == 'csv':
                print 'inside csv'
                with open(filedir+'/'+filepath) as f:
                  output = FileDatabaseInsertion.sql(filedir+'/'+filepath,filename.split('.')[0],'bigquery')
                  print output
        print strftime("%Y-%m-%d %H:%M:%S", gmtime())
        return 1

