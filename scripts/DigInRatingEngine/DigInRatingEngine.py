__author__ = 'Marlon Abeykoon'

import datetime
import modules.CommonMessageGenerator as cmg
import scripts.DigINCacheEngine.CacheController as db

class RatingEngine():

    def __init__(self, user_id, tenant, other_data=None, is_increment=True, **kwargs):
        self.user_id = user_id
        self.tenant = tenant
        self.other_data = other_data
        self.is_increment = is_increment
        self.usages = kwargs
        self.insert_obj = []

    def _calculate_summary(self):
        summary = db.get_data("SELECT parameter, value FROM digin_usage_summary "
                              "WHERE user_id = '{0}' AND tenant = '{1}'".format(self.user_id, self.tenant))
        print summary
        if summary['rows'] == ():
            db.insert_data(self.insert_obj,'digin_usage_summary')
        else:
            update_obj = []
            residue_insert = []
            for i in self.insert_obj:
                for j in summary['rows']:
                    if i['parameter'] == j[0]:
                        if self.is_increment:
                            update_obj.append({i['parameter']:str(int(j[1])+int(i['value']))})
                        else:
                            update_obj.append({i['parameter']: str(int(j[1])-int(i['value']))})
                        break
                else:
                    residue_insert.append({'parameter':i['parameter'],'value':i['value'],'user_id':self.user_id,'tenant':self.tenant})
                    continue
            if update_obj:
                for record in update_obj:
                    print record
                    db.update_data('digin_usage_summary',"WHERE parameter = '{0}' AND user_id = '{1}' AND tenant = '{2}' "
                                   .format(list(record.keys())[0], self.user_id,self.tenant), value = record.itervalues().next(),
                                   modifieddatetime=datetime.datetime.now())
            if residue_insert:
                db.insert_data(residue_insert,'digin_usage_summary')


    def get_rating_summary(self):
        summary = db.get_data("SELECT parameter, value FROM digin_usage_summary "
                              "WHERE user_id = '{0}' AND tenant = '{1}'".format(self.user_id, self.tenant))['rows']
        rated_dict = {'tenant':self.tenant,
                      'is_blocked': False}
        for parameter in summary:
            rated_dict[parameter[0]] =parameter[1]
        return cmg.format_response('True',rated_dict,"Usage data retrieved")

    def set_usage(self):

        for k,v in self.usages.items():
            usage_rating = {'user_id':self.user_id,
                       'tenant':self.tenant,
                       'parameter':k,
                       'value':v,
                       'other_data':self.other_data}
            self.insert_obj.append(usage_rating)
        print self.insert_obj
        db.insert_data(self.insert_obj,'digin_usage_details')
        self._calculate_summary()
