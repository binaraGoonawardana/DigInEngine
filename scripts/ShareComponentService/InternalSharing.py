__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.3'

import sys
import scripts.utils.AuthHandler as auth
import scripts.DigINCacheEngine as db
import modules.CommonMessageGenerator as cmg

class InternalSharing():

    def __init__(self, security_level_auth, comp_type, share_data, user_id, tenant):
        self.security_level_auth = security_level_auth
        self.type = comp_type
        self.share_data = share_data
        self.user_id = user_id
        self.tenant = tenant
        self.unauthorized_shares = []
        self.authorized_shares = []

    def __set_group_user_ids(self):

        print "User Ids retrieving from User groups.."
        for index, item in enumerate(self.share_data):
            if not item['is_user']:
                user_emails = auth.get_group_users(self.tenant, item['id'])
                for email in user_emails:
                    query = "SELECT user_id, email FROM digin_user_settings WHERE email = '{0}'".format(email['Id'])
                    user_id = db.CacheController.get_data(query)['rows'][0][0]
                    self.share_data.append({"comp_id":item['comp_id'],"is_user":True,"id":user_id,"security_level":item['security_level']})
                del self.share_data[index]

    def __is_component_owner(self, comp_id):
        query = "SELECT created_user, created_tenant FROM digin_datasource_details WHERE id = {0}".format(comp_id)
        result = db.CacheController.get_data(query)
        if result['rows'] == ():
            return False
        elif result['rows'][0][0] == self.user_id and result['rows'][0][1] == self.tenant:
            return True
        else:
            return False

    def __assign_security_level(self, comp_id, requested_security_level):
        if self.security_level_auth == 'admin' or self.__is_component_owner(comp_id):
            return requested_security_level
        else:
            return 'read'

    def __has_share_privilege(self, comp_id):
        if self.security_level_auth == 'admin' or self.__is_component_owner(comp_id):
            return True
        else:
            return False

    def do_share(self):

        self.__set_group_user_ids()
        print "Components sharing started ShareData: {0}, comp_type: {1}, Tenant: {2}".format(self.share_data,self.type,self.tenant)
        data = []
        for item in self.share_data:
            if self.__has_share_privilege(item['comp_id']):
                d = {'component_id': item['comp_id'],
                     'user_id': item['id'],
                     'type': self.type,
                     'domain': self.tenant,
                     'security_level': self.__assign_security_level(item['comp_id'], item['security_level'])}
                data.append(d)
                self.authorized_shares.append([item['comp_id'],item['id']])
            else:
                self.unauthorized_shares.append(item['comp_id'])
        try:
            db.CacheController.insert_data(data,'digin_component_access_details')
        except Exception, err:
            print err
            return cmg.format_response(False, err, "Component already shared!",exception=sys.exc_info())
        return cmg.format_response(True,{"successful_shares": self.authorized_shares, "unsuccessful_shares": self.unauthorized_shares},
                                   "Components sharing process successful")
