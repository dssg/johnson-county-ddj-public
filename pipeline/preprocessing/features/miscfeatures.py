import abstract
from ... import setup_environment
import datetime

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


class AvgDaysBetweenEvents(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "avg days between event occurences"
        self.type_of_features = 'numerical'
        self.type_of_imputation = "mean"
        self.query = ("select t1.{0}, avg(t1.diff) as avg_event_date_diff "
                      "from (select t.{0}, "
                      "date_part('day', t.begin_date -t.previous_date) as diff "
                      "from (select event1.{0}, event1.event, event1.begin_date,"
                      "LAG(event1.begin_date) over "
                      "(partition by event1.{0} order by event1.begin_date) "
                       "as previous_date from {1} event1 "
                        " where event1.begin_date < '{2}' ) "
                        "as t ) as t1 group by t1.{0} ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class StdDaysBetweenEvents(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "std days between event occurences"
        self.type_of_features = 'numerical'
        self.type_of_imputation = "mean"
        self.query = ("select t1.{0}, stddev_samp(t1.diff) as std_event_date_diff "
                      "from (select t.{0}, "
                      "date_part('day', t.begin_date -t.previous_date) as diff "
                      "from (select event1.{0}, event1.event, event1.begin_date,"
                      "LAG(event1.begin_date) over "
                      "(partition by event1.{0} order by event1.begin_date) "
                       "as previous_date from {1} event1 "
                        " where event1.begin_date < '{2}' ) "
                        "as t ) as t1 group by t1.{0} ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)

class IntersectionsPublicService(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description= 'intersections of services'
        self.type_of_features = 'categorical'
        self.query = ("select t.{0}, "
                        "(case when t.ems_count > 0 and t.mh_count> 0 and t.jims_count> 0  then 'EMS_MH_JIMS' "
                        "when t.ems_count > 0 and t.mh_count> 0 and t.jims_count <1 then 'EMS_MH'  "
                        "when t.ems_count>0 and t.jims_count> 0 and t.mh_count < 1 then 'EMS_JIMS' "
                        "when t.ems_count> 0 and t.jims_count <1 and t.mh_count < 1 then 'EMS' "
                        "when t.mh_count > 0 and t.jims_count > 0 and t.ems_count < 1 then 'MH_JIMS' "
                        "when t.mh_count > 0 and t.jims_count <1 and t.ems_count < 1 then 'MH' "
                        "when t.jims_count >0 and t.ems_count < 1 and t.mh_count < 1 then 'JIMS' "
                        "end) as service_intersections "
                        "from (select event.{0}, "
                          "sum(case when event.event like 'ems' then 1 else 0 end) as ems_count, "
                          "sum(case when event.event like 'mh' then 1 else 0 end) as mh_count, "
                          "sum(case when event.event like 'booking' then 1 else 0 end) as jims_count "
                          "from {1} event "
                          "where event.begin_date < '{2}' "
                          "group by event.{0}) as t ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)
