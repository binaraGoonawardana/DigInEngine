__author__ = 'Marlon Abeykoon'

import string
import ast
import sys
sys.path.append("...")
import modules.PostgresHandler as pg
import modules.BigQueryHandler as bq
import modules.SQLQueryHandler as mssql
from numpy import genfromtxt
import logging
import configs.ConfigHandler as conf

path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/FileUpload.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

logger.info('Starting log')
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

def sql(filepath,filename,database_type,data,data_set_name):
    """
    :param filepath: to be removed, data will be injected
    :param filename: filename without extension
    :param database_type:  BigQuery, etc
    :param data: list of lists
    :return:
    """
    datafile = filepath
    field_types ={}
    print filepath
    namefmt = '2'
    namefmt = int(namefmt)
    #outfile = os.path.basename(datafile)
    tblname = filename.replace(" ", "_") #Replace spaces with underscores in tablename/filename
    print "Tablename formatted!"
    print tblname
    # outfile = os.path.dirname(datafile) + '\\' + tblname + '.sql'

    # Create string translation tables
    allowed = ' _01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    delchars = ''
    for i in range(255):
        if chr(i) not in allowed: delchars = delchars + chr(i)
    deltable = string.maketrans(' ','_')

    # Create list of column [names],[widths]
    # reader = csv.reader(file(datafile),dialect='excel')
    # row = reader.next() # Gets first row (header of the file ['value1',value2'])
    reader = data
    row = reader[0] # gets the 1st row assuming it's the header contains fieldnames
    nc = len(row) # no of columns
    cols = []  # [['col1', 6, set(['int'])], ['col2', 7, set(['int'])], ['col3', 9, set(['int', 'string'])]]
    print "Fieldname formatting started!"
    print ', '.join(row)
    for col in row: #col = fieldname , row = header (list of fieldnames)
        # Format column name to remove unwanted chars
        col = str(string.strip(col))
        col = string.translate(col,deltable,delchars)
        if col == '': col = 'undefined' # empty fieldnames are renamed as undefined
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
        while [dupcol,1] in cols: # Add suffix to duplicate fieldnames
            d = d + 1
            dupcol = fmtcol + '_' + str(d)
        cols.append([dupcol,1])
        for col_ in cols:
            if col_[0] in field_types:
                pass
            else:
                field_types[col_[0]] = [] # field_types = {'field_name1': [], 'fieldname2': []}
    print "fieldnames formatted"
    print field_types

    # Determine max width of each column in each row
    print "Field size calculation and field type recognition started!"
    def parses_to_integer(s):
        val = ast.literal_eval(str(s))
        return isinstance(val, int) or (isinstance(val, float) and val.is_integer())
    rc = 0
    rows_bq = []
    del reader[0] # deletes the header
    for row in reader:
        #print 'inside row iterator'
        rc = rc + 1
        if len(row) == nc:
            r_dict = {}
            for i in range(len(row)):
                fld = string.strip(str(row[i]))
                if len(fld) > cols[i][1]:
                    cols[i][1] = len(fld)
                try:
                    is_int = parses_to_integer(row[i])
                    if is_int is True:
                        field_types.get(cols[i][0], []).append('int')
                    elif is_int is False:
                        field_types.get(cols[i][0], []).append('float')
                    else:
                        field_types.get(cols[i][0], []).append(type(ast.literal_eval(str(row[i]))).__name__)
                except Exception, err:
                    field_types.get(cols[i][0], []).append('string')
                    pass

                r_dict[cols[i][0]] = str(row[i])
            rows_bq.append(r_dict)
        else: print 'Warning: Line %s ignored. Different width than header' % (rc)

    for k,v in field_types.iteritems():
        if len(v) > 1:
            if 'string' in v:
                field_types[k] = 'string'
            elif 'float' in v:
                field_types[k] = 'float'
            else:
                field_types[k] = 'int'
        #field_types[k] = set(v)
        # {'float_field': set(['int', 'float']), 'int_field': set(['int']), 'string_field': set(['int', 'string'])}

    print 'Iterations done'
    #print field_types # {'Col2': set(['int']), 'Col1': set(['int'])}
    final_col = []
    for col in cols:
        col.append(field_types[col[0]])
        final_col.append(col)

    print "Field size calculation and field type recognition completed!"
    print final_col


    if database_type.lower() == 'bigquery': #[['col1', 6, set(['int'])], ['col2', 7, set(['int'])], ['col3', 9, set(['int', 'string'])]]
        schema = []
        schema_dict = {}
        for i in cols:
            schema_dict = {}
            t = i[2]
            if t == 'string':
                schema_dict['name'] = i[0]
                schema_dict['type'] = 'string'
                schema_dict['mode'] = 'nullable'
            elif t == 'int':
                schema_dict['name'] = i[0]
                schema_dict['type'] = 'integer'
                schema_dict['mode'] = 'nullable'
            elif t == 'float' or t == 'long':
                schema_dict['name'] = i[0]
                schema_dict['type'] = 'float'
                schema_dict['mode'] = 'nullable'

            schema.append(schema_dict)
        print 'Table creation started!'
        print tblname
        print schema
        try:
            print data_set_name
            result = bq.create_Table(data_set_name,tblname,schema) # ['123214', '5435323', 'isso wade']
            if result:
                print "Table creation succcessful!"
            else: print "Error occurred while creating table! If table already exists data might insert to the existing table!"
        except Exception, err:
            print "Error occurred while creating table!"
            print err
            raise
        print 'Data insertion started!'
        try:
            result = bq.Insert_Data(data_set_name,tblname,rows_bq)
            #logger.info(rows_bq)
            print "Data insertion successful!"
        except Exception, err:
            print "Error occurred while inserting data!"
            print err
            raise
        return result


    elif database_type.lower() == 'postgresql':
        print "Table creation started!"
        sql = 'CREATE TABLE %s\n(' % (tblname)
        for i in cols: # [['Col1', 6, set(['int'])], ['Col2', 7, set(['int'])], ['Col3', 9, set(['int', 'string'])]]
            t = i[2]
            if t == 'string':
                field_types[k] = 'character varying'
                sql = sql + '{0} character varying({1}),'.format(i[0],i[1]) #(Col3 character varying(set(['int', 'string']))
            elif t == 'int':
                field_types[k] = 'integer'
                sql = sql + '{0} integer,'.format(i[0]) #(Col3 character varying(set(['int', 'string']))
            elif t == 'float':
                field_types[k] = 'NUMERIC'
                sql = sql + '{0} NUMERIC({1}),'.format(i[0],i[1]) #(Col3 character varying(set(['int', 'string']))

        sql = sql[:len(sql)-1] + '\n)'
        # sql = 'CREATE TABLE %s\n(' % (tblname)
        # for col in cols:
        #     sql = sql + ('\n\t{0} NVARCHAR({1}) NULL,'.format(col[0],col[1]))
        # sql = sql[:len(sql)-1] + '\n)'
        print sql
        try:
            result = pg.execute_query(sql)
            print "Table creation successful!"
        except Exception, err:
            print err
            print "Error occurred in table creation!"
        #copy_sql = "COPY {0} FROM stdin WITH CSV HEADER DELIMITER as ','".format(tblname,)
        #f = open(datafile, 'r')
        try:
            result = pg.csv_insert(datafile,tblname,',')
            print "Data insertion successful!"
        except Exception, err:
            print err
            print "Error occurred inserting data!"
        #f.close()
    # sqlfile = open(outfile,'w')
    # sqlfile.write(sql)
    # sqlfile.close()

    # print '%s created.' % (outfile)
    elif database_type.lower() == 'mssql':
            print 'MSSQL insertion not implemented!'
            data = genfromtxt(datafile,dtype=None, delimiter=',', skiprows=1, converters={0: lambda s: str(s)})
            d = data.tolist()
            print d
            result = mssql.execute_query("INSERT INTO Price_History({0}) VALUES({1});".format('col1,col2,col3',str([d[i] for i in d]).strip('[]')))
            print result
            return result