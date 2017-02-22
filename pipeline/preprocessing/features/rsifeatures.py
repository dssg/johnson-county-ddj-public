import abstract
from ... import setup_environment
import datetime

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')

class CountOfRSI(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description =  "numbers of RSI visit"
        self.type_of_features = "numerical"
        self.type_of_imputation = "median"
        self.query = ("select rsi.{0}, count(*) as rsi_count "
                      "from {1} rsi "
                      "where rsi.admitdate < '{2}' "
                      "group by rsi.{0}").format(config_db['id_column'], config_db['rsitriage'], self.fake_today)

class AvgIntervalFromInAndDisposition(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "avg minutes of interval time from coming in RSI to disposition"
        self.type_of_features = 'numerical'
        self.type_of_imputation = "mean"
        self.query = ("select t1.{0}, avg(t1.in_mins) as rsi_avg_interval_in_and_out "
                      "from ( select rsi.{0}, "
                      "case when "
                      "(extract(hour from (rsi.timeofdisposition - rsi.timein))*60 + extract(minute from (rsi.timeofdisposition - rsi.timein))) < 0 "
                      "then "
                      "1440 + (extract(hour from (rsi.timeofdisposition - rsi.timein))*60 + extract(minute from (rsi.timeofdisposition - rsi.timein))) "
                      "else (extract(hour from (rsi.timeofdisposition - rsi.timein))*60 + extract(minute from (rsi.timeofdisposition - rsi.timein))) "
                      "end as in_mins "
                      "from {1} rsi "
                      "where rsi.admitdate < '{2}') as t1 "
                      "group by t1.{0} ").format(config_db['id_column'], config_db['rsitriage'], self.fake_today)

class ResidencyRecordedMost(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "is the county of residence Johnson County"
        self.type_of_features = "numerical"
        self.type_of_imputation = "zero"
        self.query = ("select rsi.{0}, "
                      "case when count({0}) = 0 then 0 "
                      "when (count(case when rsi.updatedcountyofresidence like '%JOHNSON' then {0} end )::float / count({0})::float) >= 0.5 "
                      "then 1 "
                      "else 0 "
                      "end as isjohnson, "
                      "case when count({0}) = 0 then 0 "
                      "when (count(case when rsi.updatedcountyofresidence like '%WYANDOTTE' then {0} end )::float / count({0})::float) >= 0.5 "
                      "then 1 "
                      "else 0 "
                      "end as iswyandotte "
                      "from {1} rsi "
                      "where rsi.admitdate < '{2}' "
                      "group by rsi.{0}").format(config_db['id_column'], config_db['rsitriage'],self.fake_today)

class TransportedBy(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "how many times have been transported by law enforcement, ambulance, CMHC, family/friend, self, and other"
        self.type_of_features = "numerical"
        self.type_of_imputation = "zero"
        self.query = ("select rsi.{0}, "
                      "count(case when rsi.updatedtransportedtorsiby like '%LAW ENFORCEMENT' then {0} end ) as rsi_num_law_enforcement_transported, "
                      "count(case when rsi.updatedtransportedtorsiby like '%AMBULANCE' then {0} end ) as rsi_num_ambulance_transported, "
                      "count(case when rsi.updatedtransportedtorsiby like '%CMHC' then {0} end ) as rsi_num_cmhc_transported, "
                      "count(case when rsi.updatedtransportedtorsiby like '%FAMILY/FRIEND' then {0} end ) as rsi_num_family_friend_transported, "
                      "count(case when rsi.updatedtransportedtorsiby like '%SELF' then {0} end ) as rsi_num_self_transported, "
                      "count(case when rsi.updatedtransportedtorsiby like '%OTHER' then {0} end ) as rsi_num_other_transported "
                      "from {1} rsi "
                      "where rsi.admitdate < '{2}' "
                      "group by rsi.{0} ").format(config_db['id_column'], config_db['rsitriage'], self.fake_today)
