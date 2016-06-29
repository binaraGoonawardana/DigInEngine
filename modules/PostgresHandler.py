__author__ = 'Sajeetharan'
import psycopg2
import psycopg2.extras
import logging
import sys
sys.path.append("...")
import configs.ConfigHandler as conf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
path_settings = conf.get_conf('FilePathConfig.ini','Logs')
path = path_settings['Path']
log_path = path + '/DiginStore.log'
handler = logging.FileHandler(log_path)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('Starting log')

try:
    datasource_settings = conf.get_conf('DatasourceConfig.ini','PostgreSQL')
    query = ""
    database = datasource_settings['DATABASE']
    user = datasource_settings['USER']
    password = datasource_settings['PASSWORD']
    host = datasource_settings['HOST']
    port = datasource_settings['PORT']
except Exception, err:
    print err
    logger.error(err)
    pass

try:
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
except psycopg2.InterfaceError as exc:
     print exc.message
     conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
     pass
except Exception, err:
     print err
     pass

logger.info('Connection made to the Digin Store Successfully')

def execute_query(query):
          records = []
          cptLigne = 0
          try:
             cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
             cur.execute(query)
             conn.commit()
             ans =cur.fetchall()
             for row in ans:
                records.append(dict(row))
          except Exception, msg:
             conn.rollback()
          return records


def get_fields(table_name,schema='public'):
        schema_name = schema
        cursor = conn.cursor()
        query = "SELECT column_name, data_type FROM information_schema.columns " \
                "WHERE table_schema = '{0}' " \
                "AND table_name = '{1}'".format(schema_name, table_name)
        cursor.execute(query)
        colnames =[]
        for desc in cursor:
           field ={'Fieldname': desc[0],
                    'FieldType': desc[1]}
           colnames.append(field)
        return colnames

def get_Tables():
          cursor = conn.cursor()
          cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
          tablesWithDetails = cursor.fetchall()
          tables =[t[0] for t in tablesWithDetails]
          return tables

def csv_insert(datafile,table_name,sep):
          # stdin.seek(0)
          cur = conn.cursor()
          # cur.copy_from(stdin, table_name, sep=',')
          copy_sql = """
           COPY {0} FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """.format(table_name)
          with open(datafile, 'w') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            cur.close()

