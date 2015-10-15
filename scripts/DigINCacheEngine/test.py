__author__ = 'Marlon'
#s = "-";
#seq = ("a", "b", "c"); # This is sequence of strings.
#print s.join( seq )

TABLE = "idinteger,"
"province character varying,"
"town character varying,"

dict = "k"

#print "INSERT INTO %s VALUES %s" % (
#    TABLE, ",".join(["(" + dict  + ")"] * 20))

args = [(1,2), (3,4), (5,6)]
print type(args)
records_list_template = ','.join(['(%s)'] * len(args))
insert_query = 'insert into t (a, b) values {0}'.format(records_list_template)
print insert_query