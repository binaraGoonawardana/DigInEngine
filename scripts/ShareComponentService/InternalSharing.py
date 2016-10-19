__author__ = 'Marlon Abeykoon'
__version__ = '1.0.0.0'

import scripts.utils.AuthHandler as auth
import scripts.DigINCacheEngine as db

class InternalSharing():

    def __init__(self, shared_party_ids_and_type, tenant, component_id):
        self.shared_party_ids_and_type = shared_party_ids_and_type
        self.tenant = tenant
        self.component_id = component_id
        self.user_ids = []
        self.group_ids = []

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


    def do_share(self):
        self._users_groups_segregator()
        self._set_group_user_ids()
        print self.user_ids
        print self.group_ids