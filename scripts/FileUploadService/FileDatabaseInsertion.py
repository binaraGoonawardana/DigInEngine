__author__ = 'Marlon Abeykoon'

import csv,string
import ast
import bigquery
import sys
sys.path.append("...")
import modules.PostgresHandler as pg
import modules.BigQueryHandler as bq
import modules.SQLQueryHandler as mssql
from numpy import genfromtxt

# if len(sys.argv)<2:
# 	print "\nUsage: csv2tbl.py path/datafile.csv (0,1,2,3 = column name format):"
# 	print "\nFormat: 0 = TitleCasedWords"
# 	print "        1 = Titlecased_Words_Underscored"
# 	print "        2 = lowercase_words_underscored"
# 	print "        3 = Words_underscored_only (leave case as in source)"
# 	sys.exit()
# else:
# 	if len(sys.argv)==2:
# 		dummy, datafile, = sys.argv
# 		namefmt = '0'
# 	else: dummy, datafile, namefmt = sys.argv

def sql(filepath,filename,database_type):
    datafile = filepath
    field_types ={}
    print filepath
    namefmt = '2'
    namefmt = int(namefmt)
    #outfile = os.path.basename(datafile)
    tblname = filename
    # outfile = os.path.dirname(datafile) + '\\' + tblname + '.sql'

    # Create string translation tables
    allowed = ' _01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    delchars = ''
    for i in range(255):
        if chr(i) not in allowed: delchars = delchars + chr(i)
    deltable = string.maketrans(' ','_')

    # Create list of column [names],[widths]
    reader = csv.reader(file(datafile),dialect='excel')
    row = reader.next()
    nc = len(row)
    cols = []  # [['col1', 6, set(['int'])], ['col2', 7, set(['int'])], ['col3', 9, set(['int', 'string'])]]
    for col in row:
        print 'inside col iterator'
        # Format column name to remove unwanted chars
        col = string.strip(col)
        col = string.translate(col,deltable,delchars)
        fmtcol = col
        if namefmt < 3:
            # Title case individual words, leaving original upper chars in place
            fmtcol = ''
            for i in range(len(col)):
                if col.title()[i].isupper(): fmtcol = fmtcol + col[i].upper()
                else: fmtcol = fmtcol + col[i]
        if namefmt == 2: fmtcol = col.lower()
        if namefmt == 0: fmtcol = string.translate(fmtcol,deltable,'_')   # Remove underscores

        d = 0
        dupcol = fmtcol
        while dupcol in cols:
            d = d + 1
            dupcol = fmtcol + '_' + str(d)
        cols.append([dupcol,1])
        for col_ in cols:
            if col_[0] in field_types:
                pass
            else:
                field_types[col_[0]] = []

    # Determine max width of each column in each row
    rc = 0
    rows_bq = []
    for row in reader:
        #print 'inside row iterator'
        rc = rc + 1
        if len(row) == nc:

            r_dict = {}
            for i in range(len(row)):
                fld = string.strip(row[i])
                if len(fld) > cols[i][1]:
                    cols[i][1] = len(fld)
                try:
                    field_types.get(cols[i][0], []).append(type(ast.literal_eval(row[i])).__name__)
                except:
                    field_types.get(cols[i][0], []).append('string')
                    pass

                r_dict[cols[i][0]] = row[i]
            rows_bq.append(r_dict)
            #print rows_bq
        else: print 'Warning: Line %s ignored. Different width than header' % (rc)
    #print field_types

    for k,v in field_types.iteritems():
        field_types[k] = set(v)
    print 'iterations done'
    #print field_types # {'Col2': set(['int']), 'Col1': set(['int'])}
    final_col = []
    for col in cols:
        col.append(field_types[col[0]])
        final_col.append(col)


    if database_type.lower() == 'bigquery': #[['col1', 6, set(['int'])], ['col2', 7, set(['int'])], ['col3', 9, set(['int', 'string'])]]
        print 'inside bq'
        schema = []
        schema_dict = {}
        for i in cols:
            schema_dict = {}
            t = next(iter(i[2]))
            if len(i[2]) > 1 or t == 'string':
                schema_dict['name'] = i[0]
                schema_dict['type'] = 'string'
                schema_dict['mode'] = 'nullable'
            elif t == 'int':
                schema_dict['name'] = i[0]
                schema_dict['type'] = 'integer'
                schema_dict['mode'] = 'nullable'

                #schema_dict[i[0]] = list(i[2])[0]
            schema.append(schema_dict)
        print 'going to create table'
        result = bq.create_Table('Demo',tblname,schema) # ['123214', '5435323', 'isso wade']
        print 'going to insert data'
        result = bq.Insert_Data('Demo',tblname,rows_bq)
        return result

    elif database_type.lower() == 'postgresql':
        print 'inside postgres'
        sql = 'CREATE TABLE %s\n(' % (tblname)
        for i in cols: # [['Col1', 6, set(['int'])], ['Col2', 7, set(['int'])], ['Col3', 9, set(['int', 'string'])]]
            t = next(iter(i[2]))
            if len(i[2]) > 1 or t == 'string':
                #print cols
                field_types[k] = 'character varying'
                sql = sql + '{0} character varying({1}),'.format(i[0],i[1]) #(Col3 character varying(set(['int', 'string']))
            elif t == 'int':
                #print cols # [['Col1', 6], ['Col2', 7]]
                field_types[k] = 'integer'
                sql = sql + '{0} integer,'.format(i[0]) #(Col3 character varying(set(['int', 'string']))
        sql = sql[:len(sql)-1] + '\n)'
        # sql = 'CREATE TABLE %s\n(' % (tblname)
        # for col in cols:
        #     sql = sql + ('\n\t{0} NVARCHAR({1}) NULL,'.format(col[0],col[1]))
        # sql = sql[:len(sql)-1] + '\n)'
        print sql
        result = pg.execute_query(sql)
        print result
        #copy_sql = "COPY {0} FROM stdin WITH CSV HEADER DELIMITER as ','".format(tblname,)
        #f = open(datafile, 'r')
        result = pg.csv_insert(datafile,tblname,',')
        #f.close()
        print result
    # sqlfile = open(outfile,'w')
    # sqlfile.write(sql)
    # sqlfile.close()

    # print '%s created.' % (outfile)
    elif database_type.lower() == 'mssql':
            data = genfromtxt(datafile,dtype=None, delimiter=',', skiprows=1, converters={0: lambda s: str(s)})
            d = data.tolist()
            print d
            result = mssql.execute_query("INSERT INTO Price_History({0}) VALUES({1});".format('col1,col2,col3',str([d[i] for i in d]).strip('[]')))
            print result
            return result