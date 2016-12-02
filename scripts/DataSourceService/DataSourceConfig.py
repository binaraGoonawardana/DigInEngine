__author__ = 'Pirinthan'
__version__ = '1.0.0.0'

import datetime
import scripts.DigINCacheEngine.CacheController as CC
import modules.CommonMessageGenerator as cmg
import json


class DataSourceConfig():
    def __init__(self,user_id,domain,data=None):
        self.userid=user_id
        self.domain=domain
        if data is not None:
            self.ds_config_id=data['ds_config_id']
            self.connectiontype=data['connection_type']
            self.connectionname=data['connection_name']
            self.hostname=data['host_name']
            self.databasename=data['database_name']
            self.port=data['port']
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
                                 'port':self.port,
                                 'user_name':self.username,
                                 'password':self.password,
                                 'others':json.dumps(self.others)}],
                                 'digin_data_source_config')
                CC.insert_data([{'component_id':DbSourceID,'user_id':self.userid,'type':'Datasource','domain':self.domain}],
                               'digin_component_access_details')

                return cmg.format_response(True,DbSourceID,"Successfully Inserted")

            except Exception , err:
                return cmg.format_response(False,str(err),'Query Failed !')


        else:
            try:
                CC.update_data('digin_data_source_config','WHERE ds_config_id ={0}'.format(self.ds_config_id),
                               connection_type=self.connectiontype,
                               connection_name=self.connectionname,
                               host_name=self.hostname,
                               database_name=self.databasename,
                               port=self.port,
                               user_name=self.username,
                               password=self.password,
                               others=json.dumps(self.others))

                return cmg.format_response(True,self.ds_config_id,"Successfully Updated")

            except Exception, err:

                return cmg.format_response(False,str(err),'Query Failed !')


    def get_datasource_config(self):

        try:
            query="SELECT ds_config_id,connection_type,connection_name,host_name,database_name,user_name,port,password,others " \
                  "FROM digin_data_source_config as a " \
                  "INNER JOIN digin_component_access_details as b ON a.ds_config_id=b.component_id " \
                  "where b.user_id='{0}' AND b.domain='{1}'".format(self.userid,self.domain)

            data=CC.get_data(query)
            list1=[]
            for x in data['rows']:
                dict1={'ds_config_id':int(x[0]),
                     'connection_type':x[1],
                     'connection_name': x[2],
                     'host_name': x[3],
                     'database_name': x[4],
                     'user_name': x[5],
                     'port': int(x[6]),
                     'password': x[7],
                     'others': json.loads(x[8])}

                list1.append(dict1)

            return cmg.format_response(True,list1,"Successfully Return")

        except Exception, err:
            return cmg.format_response(False,str(err),'Query Failed !')
