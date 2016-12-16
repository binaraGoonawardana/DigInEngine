__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.8'

import sys
import scripts.utils.AuthHandler as auth
import scripts.DigINCacheEngine as db
import modules.CommonMessageGenerator as cmg

class InternalSharing():

    def __init__(self, security_level_auth, comp_type, user_id, tenant, user_name=None, security_token=None):
        self.security_level_auth = security_level_auth
        self.type = comp_type
        self.share_data = None
        self.user_id = user_id
        self.tenant = tenant
        self.username = user_name
        self.security_token = security_token
        self.unauthorized_shares = []
        self.authorized_shares = []
        self.emails_to_send = []
        self.user_emails = set()
        self.component_names = set()

    def __is_shared(self,curr_user_id, curr_comp_id):
        query = "SELECT component_id FROM digin_component_access_details WHERE user_id = '{0}' " \
                "AND domain = '{1}' AND component_id = {2} AND type = '{3}'".format(curr_user_id, self.tenant, curr_comp_id, self.type)
        shared_status = db.CacheController.get_data(query)
        if shared_status['rows'] == ():
            return False
        else:
            return True

    def __set_group_user_ids(self):

        print "User Ids retrieving from User groups.."
        for index, item in enumerate(self.share_data):
            if not item['is_user']:
                try:
                    user_emails = auth.get_group_users(self.tenant, item['id'])
                except Exception:
                    raise
                for email in user_emails:
                    query = "SELECT user_id, email FROM digin_user_settings WHERE email = '{0}'".format(email['Id'])
                    result = db.CacheController.get_data(query)['rows']
                    if result == ():
                        print 'user not found in DigIn master DB digin_user_settings'
                    user_id = result[0][0]
                    if not self.__is_shared(user_id, item['comp_id']):
                        if not any(d['comp_id'] == item['comp_id']
                                    and str(d['id']) == str(user_id) for d in self.share_data):
                            self.share_data.append({"comp_id": item['comp_id'],"is_user": True,"id": user_id,"security_level": item['security_level'],
                                                    "user_group_id": item['id']})

    def __set_component_names(self):
        id_itr = [item['comp_id'] for item in self.share_data if item['is_user']]
        query = 'SELECT id, datasource_id FROM digin_datasource_details WHERE  id IN ({0})'.format(', '.join(id_itr))
        result = db.CacheController.get_data(query)['rows']
        tt = [value for value in result]
        self.component_names = set(tt)

    def __set_user_email(self):
        id_itr = ["'"+item['id']+"' " for item in self.share_data if item['is_user']]
        query = "SELECT user_id, email FROM digin_user_settings WHERE user_id IN ({0})".format(', '.join(id_itr))
        print query
        result = db.CacheController.get_data(query)['rows']
        tt = [value for value in result]
        self.user_emails = set(tt)

    def __is_component_owner(self, comp_id):
        if self.type == 'datasource':
            query = "SELECT created_user, created_tenant FROM digin_datasource_details WHERE id = {0}".format(comp_id)
        else:
            query = "SELECT created_user, created_tenant FROM digin_component_header WHERE digin_comp_id = {0}".format(comp_id)
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

    def do_share(self, share_data):

        self.share_data = share_data
        try:
            self.__set_group_user_ids()
        except Exception, err:
            return cmg.format_response(False, err, "Error occurred in Auth server",exception=sys.exc_info())
        self.__set_user_email()
        self.__set_component_names()
        print "Components sharing started ShareData: {0}, comp_type: {1}, Tenant: {2}".format(self.share_data,self.type,self.tenant)
        data = []
        for item in self.share_data:
            if item['is_user']:
                if self.__has_share_privilege(item['comp_id']):
                    d = {'component_id': item['comp_id'],
                         'user_id': item['id'],
                         'type': self.type,
                         'domain': self.tenant,
                         'security_level': self.__assign_security_level(item['comp_id'], item['security_level']),
                         'shared_by': self.user_id,
                         'user_group_id': item.get('user_group_id', None)}
                    data.append(d)
                    for email in self.user_emails:
                        if email[0] == item['id']:
                            for comp in self.component_names:
                                if str(comp[0]) == item['comp_id']:
                                    self.authorized_shares.append([item['comp_id'], item.get('user_group_id', item['id']), email[1], comp[1]])

                else:
                    self.unauthorized_shares.append([item['comp_id'],item.get('user_group_id', item['id'])])
        if data:
            try:
                db.CacheController.insert_data(data,'digin_component_access_details')
            except Exception, err:
                print err
                return cmg.format_response(False, err, "Component already shared!",exception=sys.exc_info())
            d ={}
            emails = set()
            datasets = set()
            for item in self.authorized_shares:
                emails.add(item[2])
                datasets.add(item[3])
                d.setdefault(item[2], [])
                d[item[2]].append(item[3])
            try:

                for key, value in d.iteritems():

                    body = "User {0} has shared dataset(s) {1} with you. \n\n".format(self.username, ', '.join(value))

                    data = {'to_addresses': key,
                            'cc_addresses': None,
                            'subject': 'DigIn - Dataset Share',
                            'from': 'Digin <noreply-digin@duoworld.com>',
                            'template_id': 'T_Email_share-dataset',
                            'default_params': {'@@Body@@': body},
                            'custom_params': {'@@Body@@': body}
                           }
                    auth.send_email(self.security_token, **data)
                    print "Email sent to %s" %key
            except Exception, err:
                print err

            try:
                body = "You have shared dataset(s) {0} with the user(s) {1}. \n\n".format(', '.join(str(s) for s in datasets), ', '.join(str(s) for s in emails))

                data = {'to_addresses': self.username,
                        'cc_addresses': None,
                        'subject': 'DigIn - Dataset Share',
                        'from': 'Digin <noreply-digin@duoworld.com>',
                        'template_id': 'T_Email_share-dataset',
                        'default_params': {'@@Body@@': body},
                        'custom_params': {'@@Body@@': body}
                        }
                auth.send_email(self.security_token, **data)
                print "Email sent to %s" % self.username
            except Exception, err:
                print err

        return cmg.format_response(True,{"successful_shares": self.authorized_shares, "unsuccessful_shares": self.unauthorized_shares},
                                   "Components sharing process successful")

    def undo_share(self, unshare_data):

        self.share_data = unshare_data
        for item in self.share_data:
            if self.__has_share_privilege(item['comp_id']):
                if item['is_user']:
                    try:
                        db.CacheController.delete_data("DELETE FROM digin_component_access_details WHERE component_id = {0} "
                                                       "AND user_id = '{1}' AND type = '{2}' AND domain = '{3}'"
                                                       .format(item['comp_id'], item['id'], self.type, self.tenant))
                    except Exception, err:
                        return cmg.format_response(False, err, "Component is not shared to undo!", exception=sys.exc_info())
                else:
                        db.CacheController.delete_data("DELETE FROM digin_component_access_details WHERE component_id = {0} "
                                                       "AND user_group_id = '{1}' AND type = '{2}' AND domain = '{3}'"
                                                       .format(item['comp_id'], item['id'], self.type, self.tenant))
            else:
                    self.unauthorized_shares.append(item['comp_id'])
        return cmg.format_response(True, {"successful_shares": self.authorized_shares,
                                          "unsuccessful_shares": self.unauthorized_shares},
                                   "Components sharing process successful")

