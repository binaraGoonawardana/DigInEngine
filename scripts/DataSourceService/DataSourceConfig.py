__author__ = 'Pirinthan'
import datetime
import scripts.DigINCacheEngine.CacheController as CC
import json


class DataSourceConfig():
    def __init__(self,data,user_id,domain):
        self.userid=user_id
        self.domain=domain
        self.ds_config_id=data['ds_config_id']
        self.connectiontype=data['connection_type']
        self.connectionname=data['connection_name']
        self.hostname=data['host_name']
        self.databasename=data['database_name']
        self.portno=data['port_no']
        self.username=data['user_name']
        self.password=data['password']
        self.others=data['others']

    def store_datasource_config(self):

        epoch = datetime.datetime.utcfromtimestamp(0)

        def unix_time_millis(dt):
            return (dt - epoch).total_seconds() * 10000.0

        if self.ds_config_id is None:
            DbSourceID = int(unix_time_millis(datetime.datetime.now()))
            print DbSourceID
            try:
                CC.insert_data([{'ds_config_id':DbSourceID,'connection_type':self.connectiontype,
                                                                'connection_name':self.connectionname,
                                                                'host_name':self.hostname,
                                                                'database_name':self.databasename,
                                                                'port_no':self.portno,
                                                                'user_name':self.username,
                                                                'password':self.password,
                                                                'others':json.dumps(self.others)}],
                                                                'digin_data_source_config')
                CC.insert_data([{'component_id':DbSourceID,'user_id':self.userid,'type':'Datasource','domain':self.domain}],
                               'digin_component_access_details')

                print "Successfully Inserted data"
                return True

            except:
                print('Insert Query return fail ...')
                return False

        else:
            try:
                CC.update_data('digin_data_source_config','WHERE ds_config_id ={0}'.format(self.ds_config_id),
                               connection_type=self.connectiontype,
                               connection_name=self.connectionname,
                               host_name=self.hostname,
                               database_name=self.databasename,
                               port_no=self.portno,
                               user_name=self.username,
                               password=self.password,
                               others=json.dumps(self.others))
                print "Successfully Updated"
                return True

            except:
                print "Update Query return fail ..."
                return False
