import abstract
from ... import setup_environment
import datetime

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


# Basic Feature
class Gender(abstract.SimpleFeature):
    def __init__(self):
        abstract.SimpleFeature.__init__(self)
        self.type_of_features = "categorical"
        self.description = "Gender of service users"
        self.query = ("""SELECT {},
                                replace(trim(both from lower(sex)),
                                        ' ', '_') as sex
                         FROM {}
                      """).format(config_db['id_column'],
                                  config_db['individuals'])

class Race(abstract.SimpleFeature):
    def __init__(self, **kwargs):
        abstract.SimpleFeature.__init__(self)
        self.type_of_features = "categorical"
        self.description = "Race of service users"
        self.query =  ("""SELECT {},
                                 replace(trim(both from lower(race)),
                                         ' ', '_') as race
                          FROM {}
                       """).format(config_db['id_column'],
                                   config_db['individuals'])

class Age(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Age of service users"
        self.query = ("select t.{}, date_part('day', '{}' - t.dob::timestamp) as age "
                      "from {} t").format(config_db['id_column'], self.fake_today, config_db['individuals'])
        self.type_of_features = 'imputation zero'

class AgeFirstInteractionPublicService(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age of earliest incident'
        self.type_of_features = 'imputation zero'
        self.query = ("select t.{0}, "
          "date_part('days', min(t.begin_date)-max(t.dob::timestamp)) as age_earliest_interaction "
          "from (select * "
          "from {1} as tmp join {3} using({0}) "
          "where tmp.begin_date < '{2}' "
          "order by tmp.begin_date) as t "
          "group by t.{0}").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today, config_db['individuals'])

class AgeFirstInteractionPublicServiceInYears(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age of earliest incident in years'
        self.type_of_features = 'imputation zero'
        self.query = ("select t.{0}, "
          "date_part('years', age(min(t.begin_date), max(t.dob::timestamp))) as age_earliest_interaction_in_years "
          "from (select * "
          "from {1} as tmp join {3} using({0}) "
          "where tmp.begin_date < '{2}' "
          "order by tmp.begin_date) as t "
          "group by t.{0}").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today, config_db['individuals'])

class AgeFirstInteractionPublicServiceDiscrete(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age of earliest incident in years'
        self.type_of_features = "categorical"
        self.query = (" select t1.{0}, "
          "case when t1.age_years <= 18 then 'less_than_18' "
          "when t1.age_years <= 34 then '19_to_34' "
          "when t1.age_years <= 44 then '35_to_44' "
          "when t1.age_years <= 54 then '45_to_54' "
          "when t1.age_years <= 64 then '55_to_64' "
          "when t1.age_years >= 65 then '65+' "
          "else null end as age_earliest_interaction_discrete "
          "from (select t.{0}, "
          "date_part('years', age(min(t.begin_date), max(t.dob::timestamp))) as age_years "
          "from (select * "
          "from {1} as tmp join {3} using({0}) "
          "where tmp.begin_date < '{2}' "
          "order by tmp.begin_date) as t "
          "group by t.{0}) as t1").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today, config_db['individuals'])



class AgeLastInteractionPublicService(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age of earliest incident'
        self.type_of_features = 'numerical'
        self.query = ("select t.{0}, "
          "date_part('days', max(t.begin_date)-max(t.dob::timestamp)) as age_lastest_interaction "
          "from (select * "
          "from {1} as tmp join {3} using({0}) "
          "where tmp.begin_date < '{2}' "
          "order by tmp.begin_date) as t "
          "group by t.{0}").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today, config_db['individuals'])

class AgeYears(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age in years'
        self.type_of_features = 'imputation zero'
        self.query = ("select p.{0}, date_part('years', age('{1}', p.dob::timestamp)) as age_years "
                      "from {2} p").format(config_db['id_column'], self.fake_today, config_db['individuals'])

class AgeDiscrete(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age in discrete groups'
        self.type_of_features = "categorical"
        self.query = ("select t.{0}, "
                      "case when t.age_years <= 18 then 'less_than_18' "
                      "when t.age_years <= 34 then '19_to_34' "
                      "when t.age_years <= 44 then '35_to_44' "
                      "when t.age_years <= 54 then '45_to_54' "
                      "when t.age_years <= 64 then '55_to_64' "
                      "when t.age_years >= 65 then '65+' "
                      "else null end as age_group "
                      "from (select p.{0}, date_part('years', age('{1}', p.dob::timestamp)) as age_years "
                      "from {2} p) as t").format(config_db['id_column'], self.fake_today, config_db['individuals'])

class AgeLastInteractionPublicServiceInYears(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age of lastest incident in years'
        self.type_of_features = 'imputation zero'
        self.query = ("select t.{0}, "
          "date_part('years', age(max(t.begin_date), max(t.dob::timestamp))) as age_last_interaction_in_years "
          "from (select * "
          "from {1} as tmp join {3} using({0}) "
          "where tmp.begin_date < '{2}' "
          "order by tmp.begin_date) as t "
          "group by t.{0}").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today, config_db['individuals'])

class AgeLastInteractionPublicServiceDiscrete(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'age of lastest incident in years'
        self.type_of_features = "categorical"
        self.query = (" select t1.{0}, "
          "case when t1.age_years <= 18 then 'less_than_18' "
          "when t1.age_years <= 34 then '19_to_34' "
          "when t1.age_years <= 44 then '35_to_44' "
          "when t1.age_years <= 54 then '45_to_54' "
          "when t1.age_years <= 64 then '55_to_64' "
          "when t1.age_years >= 65 then '65+' "
          "else null end as age_last_interaction_discrete "
          "from (select t.{0}, "
          "date_part('years', age(max(t.begin_date), max(t.dob::timestamp))) as age_years "
          "from (select * "
          "from {1} as tmp join {3} using({0}) "
          "where tmp.begin_date < '{2}' "
          "order by tmp.begin_date) as t "
          "group by t.{0}) as t1").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today, config_db['individuals'])
