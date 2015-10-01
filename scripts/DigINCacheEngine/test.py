__author__ = 'Marlon'
#s = "-";
#seq = ("a", "b", "c"); # This is sequence of strings.
#print s.join( seq )

TABLE = 'll'
dict = "k"

print "INSERT INTO %s VALUES %s" % (
    TABLE, ",".join(["(" + dict  + ")"] * 20))
