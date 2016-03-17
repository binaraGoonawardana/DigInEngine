__author__ = 'Marlon Abeykoon'

import web
import xlrd
import csv
import FileDatabaseInsertion
import ast
from time import gmtime, strftime

urls = ('/upload', 'Upload')

class Upload:
    def GET(self):
        web.header("Content-Type","text/html; charset=utf-8")
        return """<html><head></head><body>
<form method="POST" enctype="multipart/form-data" action="">
<input type="file" name="file" />
<br/>
<input type="submit" />
</form>
</body></html>"""

    def POST(self):
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
                  output = FileDatabaseInsertion.sql(filedir+'/'+filepath,filename.split('.')[0],'mssql')
                  print output
        print strftime("%Y-%m-%d %H:%M:%S", gmtime())
        return 1


if __name__ == "__main__":
   app = web.application(urls, globals())
   app.run()
