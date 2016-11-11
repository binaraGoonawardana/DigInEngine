__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.1'

import sys
import scripts.utils.AuthHandler as auth
import scripts.DigINCacheEngine as db
import modules.CommonMessageGenerator as cmg

class InternalSharing():

    def __init__(self, shared_party_ids_and_type, tenant, component_ids):
        self.shared_party_ids_and_type = shared_party_ids_and_type
        self.tenant = tenant
        self.component_ids = component_ids
        self.user_ids = []
        self.group_ids = []
        self.is_success = True

    def _users_groups_segregator(self):

        self.group_ids = [party['id'] for party in self.shared_party_ids_and_type if not party['is_user']]
        self.user_ids = [party['id'] for party in self.shared_party_ids_and_type if party['is_user']]

    def _set_group_user_ids(self):

        user_emails = []
        for _id in self.group_ids:
            user_emails.append(auth.get_group_users(self.tenant, _id))

        for email in user_emails[0]:
            query = "SELECT user_id, email FROM digin_user_settings WHERE email = '{0}'".format(email['Id'])
            user_id = db.CacheController.get_data(query)['rows']
            if user_id != ():
                self.user_ids.append(user_id[0][0])
            else:
                self.is_success = False
                return cmg.format_response(False, email, "Error sharing components no user registered in DigIn", exception=None)

    def do_share(self):

        self._users_groups_segregator()
        print "Component sharing UserIDs: {0}, GroupIds: {1}, Tenant: {2}, ComponentIds: {3}".format(self.user_ids,self.group_ids,self.tenant, self.component_ids)
        if self.group_ids:
            result = self._set_group_user_ids()
        if self.is_success:
            data = []
            for user in self.user_ids:
                for component in self.component_ids:
                    d = {'component_id': component,
                         'user_id':user,
                         'type':'dashboard',
                         'domain':self.tenant}
                    data.append(d)
            try:
                db.CacheController.insert_data(data,'digin_component_access_details')
            except Exception, err:
                print err
                return cmg.format_response(False, err, "Component already shared!",exception=sys.exc_info())
            return cmg.format_response(True,0,"Components shared successfully")
        else:
            return result

