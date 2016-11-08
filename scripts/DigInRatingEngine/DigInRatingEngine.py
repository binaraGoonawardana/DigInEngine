__author__ = 'Marlon Abeykoon'
__version__ = '1.0.1'

import datetime
import modules.CommonMessageGenerator as cmg
import scripts.DigINCacheEngine.CacheController as db
import ExceedUsageCalculator as euc

class RatingEngine():

    def __init__(self, user_id, tenant, security_level=None, other_data=None, is_increment=True, **kwargs):
        self.user_id = user_id
        self.tenant = tenant
        self.other_data = other_data
        self.is_increment = is_increment
        self.usages = kwargs
        self.insert_obj = []
        self.security_level = security_level

    def _calculate_summary(self):
        summary = db.get_data("SELECT parameter, value FROM digin_usage_summary "
                              "WHERE user_id = '{0}' AND tenant = '{1}'".format(self.user_id, self.tenant))
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
                    db.update_data('digin_usage_summary',"WHERE parameter = '{0}' AND user_id = '{1}' AND tenant = '{2}' "
                                   .format(list(record.keys())[0], self.user_id,self.tenant), value = record.itervalues().next(),
                                   modifieddatetime=datetime.datetime.now())
            if residue_insert:
                db.insert_data(residue_insert,'digin_usage_summary')


    def get_rating_summary(self):
        user_dict = {}
        if self.security_level == 'admin':
            summary = db.get_data("SELECT user_id, parameter, SUM(value) as value FROM digin_usage_summary "
                                  "WHERE tenant = '{0}' GROUP BY user_id, parameter".format(self.tenant))['rows']
        else:
            summary = db.get_data("SELECT user_id, parameter, value FROM digin_usage_summary "
                              "WHERE user_id = '{0}' AND tenant = '{1}'".format(self.user_id, self.tenant))['rows']
        for parameter in summary:
            if parameter[0] in user_dict:
                user_dict[parameter[0]][parameter[1]]=parameter[2]
            else:
                user_dict[parameter[0]] = {parameter[1]:parameter[2]}
        rated_dict = {'usage': [{self.tenant:user_dict}],
                      'exceed_blocked': euc.ExceedUsageCalculator(tenant=self.tenant, attribute=None).calculation()}
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

    def get_rating_detail(self, params):

        date_dict = {}
        if self.security_level == 'admin':
            detail = db.get_data("SELECT user_id, DATE(createddatetime), parameter, SUM(value) as value FROM digin_usage_details "
                                  "WHERE tenant = '{0}' AND DATE(createddatetime)>= {1} AND DATE(createddatetime)<= {2} "
                                  "GROUP BY user_id, DATE(createddatetime), parameter".format(self.tenant, params.start_date, params.end_date))['rows']
        else:
            detail = db.get_data("SELECT user_id, DATE(createddatetime), parameter, SUM(value) as value FROM digin_usage_details "
                                  "WHERE user_id = '{0}' AND tenant = '{1}' AND DATE(createddatetime)>= {2} AND DATE(createddatetime)<= {3} "
                                  "GROUP BY user_id, DATE(createddatetime), parameter".format(self.user_id, self.tenant, params.start_date, params.end_date))['rows']
        
        for row in detail:
            if row[0] in date_dict:
                if str(row[1]) in date_dict[row[0]]:
                    date_dict[row[0]][str(row[1])][row[2]] = row[3]
                else:
                    date_dict[row[0]][str(row[1])] = {row[2]:row[3]}
            else:
                date_dict[row[0]] = {str(row[1]):{row[2]:row[3]}}

        rated_detail = [{self.tenant:date_dict}]
        return cmg.format_response('True',rated_detail,"Usage data retrieved")
