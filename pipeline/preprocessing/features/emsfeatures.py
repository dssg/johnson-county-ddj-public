import abstract
from ... import setup_environment
import datetime

#db_tables = setup_environment.get_config_file('pipeline/default_profile.yaml')
config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


class CountOfEms(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Number of EMS visits between start_date and end_date "
        self.type_of_imputation = "mean"
        self.query = ("select ems.{0}, count(*) as ems_count "
                      "from {1} ems "
                      "where incidentdate < '{2}' "
                      "group by {0} ").format(config_db['id_column'],config_db['ems'],self.fake_today)

class LastWeekEmsCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "number of ems visits in the last week"
        last_week_date =  self.fake_today - datetime.timedelta(days = 7)
        self.query = ("select {0}, count(*) as last_week_ems_count "
                      "from {1} ems "
                      "where incidentdate < '{2}' and incidentdate > '{3}' "
                      "group by {0} ").format(config_db['id_column'], config_db['ems'], self.fake_today, last_week_date)
        self.type_of_imputation = "mean"

class LastMonthEmsCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "number of ems visits in the last month"
        last_month_date =  self.fake_today - datetime.timedelta(days = 30)
        self.query = ("select {0}, count(*) as last_month_ems_count "
                      "from {1} ems "
                      "where incidentdate < '{2}' and incidentdate > '{3}' "
                      "group by {0} ").format(config_db['id_column'], config_db['ems'], self.fake_today, last_month_date)
        self.type_of_imputation = "mean"

class LastYearEmsCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "number of ems visits in the last year"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select {0}, count(*) as last_year_ems_count "
                      "from {1} ems "
                      "where incidentdate < '{2}' and incidentdate > '{3}' "
                      "group by {0} ").format(config_db['id_column'], config_db['ems'], self.fake_today, last_year_date)
        self.type_of_imputation = "mean"

class TriageOfEms(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self, **kwargs)
        self.type_of_features = 'numerical'
        self.description = "Counts of triage color"
        self.query = ("select ems.{0}, "
                      "count(case when ems.triage like '%GREEN' then {0} end) as green_count, "
                      "count(case when ems.triage like '%YELLOW' then {0} end) as yellow_count, "
                      "count(case when ems.triage like '%RED' then {0} end) as red_count, "
                      "count(case when ems.triage like '%BLUE' then {0} end) as blue_count, "
                      "count(case when ems.triage like '%BLACK' then {0} end) as black_count ,"
                      "count(case when ems.triage is NULL then {0} end) as unknown_triage_count "
                      "from {1} ems "
                      "where incidentdate < '{2}' "
                      "group by ems.{0}".format(config_db["id_column"], config_db['ems'], self.fake_today))

class DifferentResidenceOfCityRecorded(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = 'numbers of differenct residence of city recorded'
        self.query = ("select ems.{0}, "
                      "count(distinct(ems.rescity)) as ems_num_different_rescity "
                      "from {1} ems "
                      "where ems.incidentdate < '{2}' "
                      "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)

class RefusedCareCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient refused care "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                      "count(case when ems.disposition like '%PATIENT REFUSED CARE' then {0} end) as ems_refused_care_count "
                      "from {1} ems "
                      "where ems.incidentdate < '{2}' "
                      "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)

class TreatedRefusedTransportCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient treated but refused to be transported "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                        "count(case when ems.disposition like '%TREATED AND REFUSED TRANSPORT' then {0} end) as ems_refused_transport_count "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' "
                        "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)
  
class TreatedTransferredCareCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient treated and transferred care "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                        "count(case when ems.disposition like '%TREATED  TRANSFERRED CARE' then {0} end) as ems_treated_transffered_count "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' "
                        "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)
  
class TreatedTransportedByLawEnforcementCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient treated and transported by law enforcement "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                        "count(case when ems.disposition like '%TREATED  TRANSPORTED BY LAW ENFORCEMENT' then {0} end) as ems_transported_law_enforement_count "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' "
                        "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)
  
class NoTreamentRequiredCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient required no treatment "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                        "count(case when ems.disposition like '%NO TREATMENT REQUIRED' then {0} end) as ems_no_treatment_count "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' "
                        "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)
  
class TreatedTransportedALSCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient treated as ALS "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                        "count(case when ems.disposition like '%TREATED  TRANSPORTED BY EMS (ALS)' then {0} end) as ems_als_count "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' "
                        "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)
  
class TreatedTransportedBLSCount(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "counts of patient treated as BLS "
        self.type_of_features = "numerical"
        self.query = ("select ems.{0},"
                        "count(case when ems.disposition like '%TREATED  TRANSPORTED BY EMS (BLS)' then {0} end) as ems_bls_count "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' "
                        "group by ems.{0} ").format(config_db['id_column'], config_db['ems'], self.fake_today)
  
class Homelessness(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "is marked as homeless in the ems data"
        self.type_of_features = 'imputation zero'
        self.query = ("select ems.{0}, max(case when ems.homeless= false then 0 else 1 end) as homelessness "
                      "from {1} ems "
                      "where ems.incidentdate < '{2}' "
                      "group by ems.{0}").format(config_db['id_column'], config_db['ems'], self.fake_today)

class EverHomelessness(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "is marked as ever homeless in the ems data"
        self.type_of_features = 'imputation zero'
        self.query = ("select ems.{0}, max(case when ems.ever_homeless= false then 0 else 1 end) as ever_homelessness "
                      "from {1} ems "
                      "where ems.incidentdate < '{2}' "
                      "group by ems.{0}").format(config_db['id_column'], config_db['ems'], self.fake_today)

class TreatedSum (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "is marked as treated in the ems data"
        self.type_of_features = 'imputation zero'
        self.query = ("select ems.{0}, sum(case when ems.treated= false then 0 else 1 end) as treated_sum "
                      "from {1} ems "
                      "where ems.incidentdate < '{2}' "
                      "group by ems.{0}").format(config_db['id_column'], config_db['ems'], self.fake_today)

class TransportedSum (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "is marked as transported in the ems data"
        self.type_of_features = 'imputation zero'
        self.query = ("select ems.{0}, sum(case when ems.transported = false then 0 else 1 end) as transported_sum "
                      "from {1} ems "
                      "where ems.incidentdate < '{2}' "
                      "group by ems.{0}").format(config_db['id_column'], config_db['ems'], self.fake_today)

class Destination (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "ems destination"
        self.type_of_features = 'categorical'
        self.query = ("select ems.{0}, "
                        "case when ems.destination like '%SHAWNEE MISSION MEDICAL CENTER%' then 'shawnee' "
                        "when ems.destination like '%OLATHE MEDICAL CENTER%' then 'olathe' "
                        "when ems.destination like '%OVERLAND PARK REGIONAL MEDICAL CENTER%' then 'overland_park' "
                        "when ems.destination like '%MENORAH MEDICAL PARK%' then 'menorah_medical_park' "
                        "when ems.destination like '%ST. LUKES SOUTH%' then 'st_lukes_south ' "
                        "when ems.destination like '%KANSAS UNIVERSITY MEDICAL CENTER%' then 'kansas_university' "
                        "when ems.destination like '%ST. JOSEPH HOSPITAL%' then 'st_joseph_hosp' "
                        "when ems.destination like '%ST. LUKES HOSPITAL%' then 'st_lukes_hosp' "
                        "when ems.destination like '%CHILDRENS MERCY%' then 'childrens_mercy_south' "
                        "when ems.destination is null then null "
                        "else 'other' end as destination "
                        "from {1} ems "
                        "where ems.incidentdate < '{2}' ").format(config_db['id_column'], config_db['ems'], self.fake_today)

class PrimaryImpression(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = 'primary impression for ems'
        self.type_of_features = 'categorical'
        self.query = ("select ems.{0}, "
          "case when ems.primaryimpression like '%TRAUMATIC INJURY%' then 'traumatic_injury' "
          "when ems.primaryimpression like '%BEHAVIORAL/PSYCHIATRIC DISORDER%' then 'behavioral_psychiatric' "
          "when ems.primaryimpression like '%POISONING/DRUG INGESTION%' then 'poisoning_drug_ingestion' "
          "when ems.primaryimpression like '%CHEST PAIN/DISCOMFORT%' then 'chest_pain_discomfort' "
          "when ems.primaryimpression like '%ABDOMINAL PAIN/PROBLEMS%' then 'abdominal_pain_problems' "
          "when ems.primaryimpression like '%PAIN%' then 'pain' "
          "when ems.primaryimpression like '%SEIZURE%' then 'seizure' "
          "when ems.primaryimpression like '%ETOH ABUSE%' then 'etoh_abuse' "
          "when ems.primaryimpression is null then null "
          "else 'other' end  as primary_impression "
          "from {1} ems "
          "where ems.incidentdate < '{2}'").format(config_db['id_column'], config_db['ems'], self.fake_today)
