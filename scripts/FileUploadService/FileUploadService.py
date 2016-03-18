__author__ = 'Marlon Abeykoon'

import web
import xlrd
import FileDatabaseInsertion
from time import gmtime, strftime

urls = ('/file_upload', 'Upload')


def file_upload(params):
        web.header('enctype','multipart/form-data')
        print strftime("%Y-%m-%d %H:%M:%S", gmtime())
        x = web.input(file={})
        filedir = '/DiginUploads' # change this to the directory you want to store the file in.
        if 'file' in x: # to check if the file-object is created
            filepath=x.file.filename.replace('\\','/') # replaces the windows-style slashes with linux ones.
            filename=filepath.split('/')[-1] # splits the and chooses the last part (the filename with extension)
            fout = open(filedir +'/'+ filename,'w') # creates the file where the uploaded file should be stored
            fout.write(x.file.file.read()) # writes the uploaded file to the newly created file.
            fout.close() # closes the file, upload complete.
            extension = filename.split('.')[-1]
            print extension
            if extension == 'xlsx':
                # Open the workbook
                print filedir+'/'+filepath
                xl_workbook = xlrd.open_workbook(filedir+'/'+filepath)
                xl_sheet = xl_workbook.sheet_by_index(0)
                row = xl_sheet.row(0)
                from xlrd.sheet import ctype_text

            elif extension == 'csv':
                print 'inside csv'
                with open(filedir+'/'+filepath) as f:
                  i = 0
                  type_dict= {}
                  # for col in row2:
                  #     col1_type = ast.literal_eval(col)
                  #     print col1_type
                  #     print type(col1_type)
                  #     type_dict[row1[i]] = type(col1_type)
                  #     i += 1
                  output = FileDatabaseInsertion.sql(filedir+'/'+filepath,filename.split('.')[0],'bigquery')
                  print output
        print strftime("%Y-%m-%d %H:%M:%S", gmtime())
        return 1

