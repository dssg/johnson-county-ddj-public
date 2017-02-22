import abstract
from ... import setup_environment
import datetime

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


class CountOfMentalHealth(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of Mental Health visits between start_date and end_date"
        self.type_of_features = 'numerical'
        self.query = ("select {0}, count(*) as mh_count "
                      "from {1} mh "
                      "where admit_date < '{2}' "
                      "group by {0} ").format(config_db['id_column'],config_db['mental_health'],self.fake_today)
        self.type_of_imputation = "mean"

class LastWeekMhCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Number of Mental Health visits in the last week"
        self.type_of_features = 'numerical'
        last_week_date =  self.fake_today - datetime.timedelta(days = 7)
        self.query = ("select {0}, count(*) as last_week_mh_count "
                      "from {1} mh "
                      "where admit_date < '{2}' and admit_date > '{3}' "
                      "group by {0} ").format(config_db['id_column'],config_db['mental_health'],self.fake_today, last_week_date)
        self.type_of_imputation = "mean"

class LastMonthMhCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Number of Mental Health visits in the last month"
        self.type_of_features = 'numerical'
        last_month_date =  self.fake_today - datetime.timedelta(days = 30)
        self.query = ("select {0}, count(*) as last_month_mh_count "
                      "from {1} mh "
                      "where admit_date < '{2}' and admit_date > '{3}' "
                      "group by {0} ").format(config_db['id_column'],config_db['mental_health'],self.fake_today, last_month_date)

class LastYearMhCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Number of Mental Health visits in the last week"
        self.type_of_features = 'numerical'
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select {0}, count(*) as last_year_mh_count "
                      "from {1} mh "
                      "where admit_date < '{2}' and admit_date > '{3}' "
                      "group by {0} ").format(config_db['id_column'],config_db['mental_health'],self.fake_today, last_year_date)
        self.type_of_imputation = "mean"


class LastYearMhDaysAvg (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Average number of days enrolled in mental health in past year"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.type_of_imputation = "mean"
        self.query = ("select mh.{0}, "
                      "avg(date_part('day', end_date - begin_date)) as mh_len_avg_year "
                      "from ( select {0}, "
                      "case when dschrg_date < '{2}' then dschrg_date "
                      "else '{2}' end as end_date,"
                      "case when admit_date > '{3}' then admit_date "
                      "else '{3}' end as begin_date, "
                      "dschrg_date, admit_date "
                      "from {1} "
                      "where admit_date < '{2}' and (dschrg_date> '{3}' or dschrg_date is null) "
                      ") as mh "
                      "group by mh.{0} ").format(config_db['id_column'],config_db['mental_health'],self.fake_today, last_year_date)


class LastYearMhDaysSum (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Total number of days enrolled in mental health in past year"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.type_of_imputation = "mean"
        self.query = ("select mh.{0}, "
                      "sum(date_part('day', end_date - begin_date)) as mh_len_sum_year "
                      "from ( select {0}, "
                      "case when dschrg_date < '{2}' then dschrg_date "
                      "else '{2}' end as end_date,"
                      "case when admit_date > '{3}' then admit_date "
                      "else '{3}' end as begin_date, "
                      "dschrg_date, admit_date "
                      "from {1} "
                      "where admit_date < '{2}' and (dschrg_date> '{3}' or dschrg_date is null) "
                      ") as mh "
                      "group by mh.{0} ").format(config_db['id_column'], config_db['mental_health'],self.fake_today, last_year_date)

class LastYearMhDaysStddev (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Standard deviation of mental health enrollment length in past year"
        self.type_of_imputation = "mean"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select mh.{0}, "
                      "stddev_samp(date_part('day', end_date - begin_date)) as mh_len_stddev_year "
                      "from ( select {0}, "
                      "case when dschrg_date < '{2}' then dschrg_date "
                      "else '{2}' end as end_date,"
                      "case when admit_date > '{3}' then admit_date "
                      "else '{3}' end as begin_date, "
                      "dschrg_date, admit_date "
                      "from {1} "
                      "where admit_date < '{2}' and (dschrg_date> '{3}' or dschrg_date is null) "
                      ") as mh "
                      "group by mh.{0} ").format(config_db['id_column'], config_db['mental_health'],self.fake_today, last_year_date)

class ImportantDiagnoses(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'categorical'
        self.description = "important possible mh diagnoses"
        self.query = ("select mh.{0},"
                      "case when mh.pri_dx_value like '%UNSPECIFIED AFFECTIVE PSYCHOSIS%' then 'unspecified_affective_psychosis' "
                      "when mh.pri_dx_value like '%MANIC%' then 'manic' "
                      "when mh.pri_dx_value like '%DEPRESSIVE%' then 'depressive' "
                      "when mh.pri_dx_value like '%PYSCHOSIS%' then 'pyschosis' "
                      "when mh.pri_dx_value like '%BIPOLAR%' then 'bipolar' "
                      "when mh.pri_dx_value like '%SCHIZO%' then 'schizo' "
                      "when mh.pri_dx_value like '%ANXIETY%' then 'anxiety' "
                      "when mh.pri_dx_value is not NULL then 'other' "
                      "else null end as mh_diagnose_main "
                      "from {1} mh "
                      "where mh.admit_date < '{2}' ").format(config_db["id_column"], config_db["mental_health"], self.fake_today)

class Referral(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'categorical'
        self.description = "who referred to mh center"
        self.query= ("select mh.{0}, "
              "case when mh.refferal_source like '%SELF%' then 'self' "
               "when mh.refferal_source like '%FAMILY & RELATIVES%' then 'other_person' "
               "when mh.refferal_source like '%NO ENTRY%' then 'no_entry' "
               "when mh.refferal_source like '%COURT%' then 'crime_justice' "
               "when mh.refferal_source like '%HOSPITAL%' then 'hospital' "
               "when mh.refferal_source like '%CORRECITONS%' then 'crime_justice' "
               "when mh.refferal_source like '%PROBATION%' then 'crime_justice' "
               "when mh.refferal_source like '%POLICE%' then 'police' "
               "when mh.refferal_source like '%PHYSICIAN%' then 'physician' "
               "when mh.refferal_source like '%DRUG%' then 'drug_treatment' "
               "when mh.refferal_source like '%FRIENDS%' then 'other_person' "
               "when mh.refferal_source like '%UNKNOWN%' then 'unknown' "
               "when mh.refferal_source like '%PENAL SYSTEM%' then 'crime_justice' "
               "when mh.refferal_source like '%PAROLE%' then 'crime_justice' "
               "when mh.refferal_source like '%JUVENILE JUSTICE AUTHORITY%' then 'crime_justice' "
               "when mh.refferal_source is not null then 'other' "
              "else null end as mh_referral "
              "from {1} mh "
              "where mh.admit_date < '{2}'").format(config_db["id_column"], config_db["mental_health"], self.fake_today)

class Program(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "program involvement for mh"
        self.query = ("select mh.{0}, "
                        "count(case when mh.program like '%OUTPATIENT%' then {0} end) as outpatient_count, "
                        "count(case when mh.program like '%ADULT MH PROGRAM%' then {0} end) as adult_mh_program_count, "
                        "count(case when mh.program like '%EMERGENCY SRV%' then {0} end) as emergency_srv_count, "
                        "count(case when mh.program like '%CSS PGM%' then {0} end) as css_program_count, "
                        "count(case when mh.program like '%MEDICATION ONLY%' then {0} end) as medication_only_count, "
                        "count(case when mh.program like '%FF OUTPATIENT%' then {0} end) as ff_outpatient_count, "
                        "count(case when mh.program like '%ADMISSION PROGRAM%' then {0} end) as admission_program_count, "
                        "count(case when mh.program like '%FORENSIC%' then {0} end) as forensic_count, "
                        "count(case when mh.program like '%SEXUAL ABUSE%' then {0} end) as sexual_abuse_count, "
                        "count(case when mh.program like '%FAMILY FOCUS%' then {0} end) as family_focus_count "
                        "from {1} mh "
                        "where mh.admit_date < '{2}' "
                        "group by mh.{0}").format(config_db["id_column"], config_db["mental_health"], self.fake_today)


class Diagnoses(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'categorical'
        self.description = "diagnoses for mh"
        self.query = ("select mh.{0}, "
                      "case when d.dx_description like '%MOOD DISORDER NOS%' then 'mood_disorder' "
                      "when d.dx_description like '%ATTENTION-DEFICIT/HYPERACTIVITY DISORDER, COMBINED TYPE%' then 'attention deficit/hyperactivity' "
                      "when d.dx_description like '%ANXIETY DISORDER NOS%' then 'anxiety_disorder' "
                      "when d.dx_description like '%BIPOLAR DISORDER NOs%' then 'bipolar_disorder' "
                      "when d.dx_description like '%STRESS DISORDER%' then 'stress_disorder' "
                      "when d.dx_description like '%GENERALIZED ANXIETY DISORDER%' then 'generalized_anxiety_disorder' "
                      "when d.dx_description like '%OPPOSITIONAL DEFIANT DISORDER%' then 'oppositional_defiant_disorder' "
                      "when d.dx_description like '%DEPRESSIVE DISORDER%' then 'depressive_disorder' "
                      "when d.dx_description is null then null "
                      "else 'other' end as mh_diagnoses "
                      "from {1} mh , {2} d "
                      "where mh.patid = d.patid "
                      " and mh.admit_date < '{3}' and d.dx_date < '{3}'").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_diagnoses"], self.fake_today)


class NumOfMHServices(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "number of services used for patient"
        self.query = ("select mh.{0}, count(s.service_description) as mh_num_services "
                      "from {1} mh, {2} s "
                      "where mh.patid =s.patid and mh.admit_date < '{3}' and s.svc_date < '{3}' "
                      "group by mh.{0} ").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_services"], self.fake_today)




class NumOfUniqueMHServices(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "number of unique services used for patient"
        self.query = ("select mh.{0}, count(distinct s.service_description) as mh_number_unique_services "
                      "from {1} mh, {2} s "
                      "where mh.patid =s.patid and mh.admit_date < '{3}' and s.svc_date < '{3}' "
                      "group by mh.{0}").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_services"], self.fake_today)


class Discharge(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'categorical'
        self.description = "reason for discharge"
        self.query = ("select mh.{0}, "
                      "case when d.discharge_reason like '%DROPPED OUT%'  then 'dropped_out' "
                      "when d.discharge_reason like '%EVALUATION COMPLETED%'  then 'evaluation_complete' "
                      "when d.discharge_reason like '%TX NOT COMPLETE CLIENT DECISION%'  then 'tx_not_complete_client_decision' "
                      "when d.discharge_reason like '%CLIENT MOVED%'  then 'client_moved' "
                      "when d.discharge_reason like '%TRANSFER TO TX OUTSIDE MHC%'  then 'transfer_to_tx_outside_mhc' "
                      "when d.discharge_reason like '%TX NOT COMPLETE AGCY DECISION%'  then 'tx_not_complete_agcy_decision' "
                      "when d.discharge_reason like '%TRANSFER TO TX OUTSIDE MHC%'  then 'transfer_to_tx_outside_mhc' "
                      "when d.discharge_reason like '%BY/TO COURT OR TO JAIL%'  then 'by_to_court_or_jail' "
                      "when d.discharge_reason like '%TREATMENT REJECTED%'  then 'treatment_rejected' "
                      "when d.discharge_reason is null then null "
                      "else 'other' end as mh_discharge "
                      "from {1} mh, {2} d "
                      "where mh.patid = d.patid and mh.admit_date < '{3}' "
                      "and d.dschg_date < '{3}'").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_dschg"], self.fake_today)

class NumberOfTherapists(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "number of therapists"
        self.query = ("select mh.{0}, count(distinct s.therapist_num) as num_therapist "
                      "from {1} mh , {2} s "
                      "where mh.patid = s.patid "
                      "and mh.admit_date < '{3}' and s.svc_date < '{3}' "
                      "group by mh.{0}").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_services"], self.fake_today)

class ServicesRecieved(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'categorical'
        self.description = "did the person recieve this type of services"
        self.query= ("select mh.{0}, "
                      "case when s.service_description like '%CLIENT/COLLATERAL PH CALL%' then 'client/collateral_ph_call' "
                      "when s.service_description like '%60 MIN IND/FAM - 120 MIN GRP UNKEPT%' then '60_min_ind_fam_120' "
                      "when s.service_description like '%CPST STRENGTH BASED CASE MNGMNT (EBP)%' then 'cpst_strength_based_case' "
                      "when s.service_description like '%TARGETED CASE MANAGEMENT%' then 'targeted_case_management' "
                      "when s.service_description like '%INDIVIDUAL THERAPY%' then 'individual_therapy' "
                      "when s.service_description like '%NO BILL CLIENT AND COLLATERAL PHONE CAL%' then 'no_bill_client_and_collateral' "
                      "when s.service_description like '%COMMUNITY PSYCH SUPPORT (CPST)-CHILD%' then 'community_psych_support' "
                      "when s.service_description like '%STRENGTH BASED CASE MGMT%' then 'strength_based_case' "
                      "when s.service_description like '%TRANSPORTATION SERVICES%' then 'transportation_services' "
                      "when s.service_description like '%MH ATTENDANT CARE%' then 'mh_attendant' "
                      "when s.service_description like '%TCM - TARGETED CASE MANAGEMENT%' then 'tcm_targeted_case_management' "
                      "when s.service_description is null then null "
                      "else 'other' end as mh_service_desc "
                      "from {1} mh, {2} s "
                      "where mh.patid = s.patid and "
                      "mh.admit_date < '{3}'  and s.svc_date < '{3}'").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_services"], self.fake_today)

class MostCommonTherapistNumber(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "which therapist was seen the most by person"
        self.query = ("select mh.{0}, mode() within group (order by s.therapist_num) as mh_therapist_mode "
                      "from {1} mh, {2} s "
                      "where s.patid = mh.patid and "
                      "mh.admit_date < '{3}' and s.svc_date < '{3}' "
                      "group by mh.{0}").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_services"], self.fake_today)


class ProgramsDischarges(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'categorical'
        self.description = "divides the program and discharge into categories"
        self.query = ("select mh.{0},  "
                      "case when d.program like '%FAMILY FOCUS OUTPATIENT%' and d.discharge_reason is null then 'family_focus_null_discharge' "
                      "when d.program like '%OUTPATIENT%' and d.discharge_reason like '%DROPPED OUT%' then 'outpatient_dropped_out' "
                      "when  d.program like '%EMERGENCY SERVICES%' and d.discharge_reason is null then 'emergency_service_null_discharge' "
                      "when d.program like '%AFTER HOURS SERVICES%' and d.discharge_reason is null then 'after_hours_services_null_discharge' "
                      "when d.program like '%PRE ADMIT HOSPITAL SCREENING%' and d.discharge_reason  like '%EVALUATION COMPLETED%' then 'pre_admit_hosptial_evaluation_complete' "
                      "when d.program like '%FORENSIC SERVICES%' and d.discharge_reason is null then 'forensic_services_null_discharge' "
                      "when d.program like '%MOBILE CRISIS RESPONSE TEAM%' and d.discharge_reason is null then 'mobile_crisis_team_null_discharge' "
                      "when d.program like '%ADULT MH PROGRAM%' and d.discharge_reason like '%TX NOT COMPLETE CLIENT DECISION%' then 'adult_mh_program_tx_not_complete_client_decision' "
                      "when d.program like '%EMERGENCY SERVICES%' and d.discharge_reason like '%DROPPED OUT%' then 'emergency_services_dropped_out' "
                      "when d.program like '%CSS PROGRAM%' and d.discharge_reason like '%DROPPED OUT%' then 'css_dropped_out' "
                      "else 'other_program_discharge' end as program_discharge_combo "
                      "from {1} mh, {2} d "
                      "where mh.patid = d.patid "
                      "and mh.admit_date < '{3}' and d.dschg_date < '{3}'").format(config_db["id_column"], config_db["mental_health"],config_db["mental_health_dschg"], self.fake_today)

