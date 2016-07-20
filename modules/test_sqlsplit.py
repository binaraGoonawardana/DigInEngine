__author__ = 'Manura Omal Bhagya'

import sqlalchemy as s
import json

def sqlsplit(dbname,policyClient,field1,field2,field3,delimiter):
    # field = 'ClientAddress'
    # dbname = 'DimPolicy'

    # delimiter = "' '"
    engine = s.create_engine("mssql+pyodbc://smsuser:sms@192.168.1.83:1433/HNBDB?driver=SQL+Server+Native+Client+10.0")
    metadata = s.MetaData()
    connection = engine.connect()

    try:

        s.event.listen(
            metadata,
            'before_create',
            s.schema.DDL("CREATE FUNCTION split \
                        (\
                            @string varchar(MAX),\
                            @delimiter CHAR(1),\
                            @pos INT\
                        )\
                        RETURNS varchar(255)\
                        AS \
                        BEGIN \
                            DECLARE @start INT, @end INT, @count INT \
                            SELECT @start = 1, @end = CHARINDEX(@delimiter, @string), @count = 1 \
                            WHILE @start < LEN(@string) + 1 BEGIN \
                                IF @end = 0 \
                                    SET @end = LEN(@string) + 1 \
                                IF @count = @pos \
                                    RETURN SUBSTRING(@string, @start, @end - @start) \
                                SET @start = @end + 1 \
                                SET @end = CHARINDEX(@delimiter, @string, @start) \
                                SET @count = @count + 1 \
                            END \
                            RETURN '' \
                        END"
        )
        )
        metadata.create_all(engine)

    except s.exc.ProgrammingError:
        pass

    query = "SELECT	{5},str({3},8,2) val1,{4} val2,dbo.split({0}, {2}, (len({0}) - len(replace({0}, ' ', '')) + 1)) city1,\
            dbo.split({0}, {2}, (len({0}) - len(replace({0}, ' ', '')))) city2,\
            dbo.split({0}, {2}, (len({0}) - len(replace({0}, ' ', '')) - 1)) city3,\
            dbo.split({0}, {2}, (len({0}) - len(replace({0}, ' ', '')) - 2)) city4\
            FROM {1}".format(field1,dbname,delimiter,field2,field3,policyClient)

    result = connection.execute(query)
    dict = {}
    #   x = 1
    for row in result:
        add = row['city4'],row['city3'],row['city2'],row['city1']
        dict2 = {row['policyClient']:{"Address": add ,"val1":row['val1'],"val2":row['val2']}}
        dict.update(dict2)
    connection.close()

    return json.dumps(dict)
#sqlsplit(dbname,policyClient,field1,field2,field3,delimiter)

#print sqlsplit('DimPolicy','policyClient','ClientAddress','SumInsured','CoverType',"' '")