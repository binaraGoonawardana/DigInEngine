__author__ = 'Marlon'

import xlrd

def open_file(path):
    print path
    book = xlrd.open_workbook(path)
    nsheets =  book.nsheets
    #sheetnames =  book.sheet_names()
    for sheets in range(0, nsheets) :
        sheet = book.sheet_by_index(sheets)
        return sheet.col_values(0)


# if __name__ == "__main__":
#     path = "test.xlsx"
#     open_file(path)