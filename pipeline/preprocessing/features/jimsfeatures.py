import abstract
from ... import setup_environment
import datetime

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


 # Jail Booking Feature
class AvgBailAmount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Average Bail amount"
        self.type_of_features = "numerical"
        self.type_of_imputation = "mean"
        self.query = ("select bail.{0}, avg(bail.bail_pay::float) as avg_bail_amt "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today)

class LastYearAvgBailAmount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Average bail amount in the last year"
        self.type_of_features = "numerical"
        self.type_of_imputation = "mean"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select bail.{0}, avg(bail.bail_pay::float) as avg_bail_amt_last_year "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' and bail.receipt_date > '{3}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today, last_year_date )

class LastMonthAvgBailAmount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Average bail amount in the last month"
        self.type_of_features = "numerical"
        self.type_of_imputation = "mean"
        last_month_date =  self.fake_today - datetime.timedelta(days = 30)
        self.query = ("select bail.{0}, avg(bail.bail_pay::float) as avg_bail_amt_last_month "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' and bail.receipt_date > '{3}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today, last_month_date )

class LastWeekAvgBailAmount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.description = "Average bail amount in the last week"
        self.type_of_features = "numerical"
        self.type_of_imputation = "mean"
        last_week_date =  self.fake_today - datetime.timedelta(days = 7)
        self.query = ("select bail.{0}, avg(bail.bail_pay::float) as avg_bail_amt_last_week "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' and bail.receipt_date > '{3}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today, last_week_date )

class BailedOutCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 2
        self.type_of_imputation = "zero"
        self.description = "Counts of bailed out or not"
        self.query = ("select bail.{0}, "
                      "count(case when bail.bail_out = 'true' then {0} end) as bailed_out_true_counts, "
                      "count(case when bail.bail_out = 'false' then {0} end) as bailed_out_false_counts "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' "
                      "group by bail.{0} ").format(config_db['id_column'], config_db['jimsbail'], self.fake_today)

class BailTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 5
        self.type_of_imputation = "zero"
        #self.name_of_features = ["pr_count", "sur_count", "ca_count", "orcd_count", "gps_count", "pps_count"]
        self.description = "counts of bail type"
        self.query = ("select bail.{0}, "
                      "count(case when bail.bail_type like '%PR' then {0} end) as jims_bail_pr_count, "
                      "count(case when bail.bail_type like '%SUR' then {0} end) as jims_bail_sur_count, "
                      "count(case when bail.bail_type like '%CA' then {0} end) as jims_bail_ca_count, "
                      "count(case when bail.bail_type like '%ORCD' then {0} end) as jims_bail_orcd_count, "
                      "count(case when bail.bail_type like '%GPS' then {0} end) as jims_bail_gps_count "
                      "from {1} as bail "
                      "where receipt_date < '{2}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today)

class LastYearBailTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 5
        self.type_of_imputation = "zero"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        #self.name_of_features = ["pr_count", "sur_count", "ca_count", "orcd_count", "gps_count", "pps_count"]
        self.description = "counts of bail type in the last year"
        self.query = ("select bail.{0}, "
                      "count(case when bail.bail_type like '%PR' then {0} end) as jims_bail_pr_count_last_year, "
                      "count(case when bail.bail_type like '%SUR' then {0} end) as jims_bail_sur_count_last_year, "
                      "count(case when bail.bail_type like '%CA' then {0} end) as jims_bail_ca_count_last_year, "
                      "count(case when bail.bail_type like '%ORCD' then {0} end) as jims_bail_orcd_count_last_year, "
                      "count(case when bail.bail_type like '%GPS' then {0} end) as jims_bail_gps_count_last_year "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' and bail.receipt_date > '{3}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today, last_year_date )

class LastMonthBailTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 5
        self.type_of_imputation = "zero"
        last_month_date =  self.fake_today - datetime.timedelta(days = 30)
        #self.name_of_features = ["pr_count", "sur_count", "ca_count", "orcd_count", "gps_count", "pps_count"]
        self.description = "counts of bail type in the last month"
        self.query = ("select bail.{0}, "
                      "count(case when bail.bail_type like '%PR' then {0} end) as jims_bail_pr_count_last_month, "
                      "count(case when bail.bail_type like '%SUR' then {0} end) as jims_bail_sur_count_last_month, "
                      "count(case when bail.bail_type like '%CA' then {0} end) as jims_bail_ca_count_last_month, "
                      "count(case when bail.bail_type like '%ORCD' then {0} end) as jims_bail_orcd_count_last_month, "
                      "count(case when bail.bail_type like '%GPS' then {0} end) as jims_bail_gps_count_last_month "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' and bail.receipt_date > '{3}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today, last_month_date )

class LastWeekBailTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 5
        self.type_of_imputation = "zero"
        last_week_date =  self.fake_today - datetime.timedelta(days = 7)
        #self.name_of_features = ["pr_count", "sur_count", "ca_count", "orcd_count", "gps_count", "pps_count"]
        self.description = "counts of bail type in the last week"
        self.query = ("select bail.{0}, "
                      "count(case when bail.bail_type like '%PR' then {0} end) as jims_bail_pr_count_last_week, "
                      "count(case when bail.bail_type like '%SUR' then {0} end) as jims_bail_sur_count_last_week, "
                      "count(case when bail.bail_type like '%CA' then {0} end) as jims_bail_ca_count_last_week, "
                      "count(case when bail.bail_type like '%ORCD' then {0} end) as jims_bail_orcd_count_last_week, "
                      "count(case when bail.bail_type like '%GPS' then {0} end) as jims_bail_gps_count_last_week "
                      "from {1} as bail "
                      "where bail.receipt_date < '{2}' and bail.receipt_date > '{3}' "
                      "group by bail.{0}").format(config_db['id_column'], config_db['jimsbail'], self.fake_today, last_week_date )


class CaseTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 3
        self.type_of_imputation = "zero"
        self.description = "Counts of case type: CR, DV and JV"
        self.query = ("select casetable.{0}, "
                      "count(case when casetable.case_type like '%CR' then {0} end) as jims_case_cr_count, "
                      "count(case when casetable.case_type like '%DV' then {0} end) as jims_case_dv_count, "
                      "count(case when casetable.case_type like '%JV' then {0} end) as jims_case_jv_count, "
                      "count(case when casetable.case_type like '%TC' then {0} end) as jims_case_tc_count, "
                      "count(case when casetable.case_type like '%TR' then {0} end) as jims_case_tr_count, "
                      "count(case when casetable.case_type like '%JC' then {0} end) as jims_case_jc_count, "
                      "count(case when casetable.case_type like '%CC' then {0} end) as jims_case_cc_count, "
                      "count(case when casetable.case_type like '%FG' then {0} end) as jims_case_fg_count "
                      "from {1} as casetable "
                      "where casetable.first_court_date < '{2}' "
                      "group by casetable.{0} ").format(config_db['id_column'], config_db['jimscase'], self.fake_today)

class LastYearCaseTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 3
        self.type_of_imputation = "zero"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.description = "Counts of case type: CR, DV and JV in the last year"
        self.query = ("select casetable.{0}, "
                      "count(case when casetable.case_type like '%CR' then {0} end) as jims_case_cr_count_last_year, "
                      "count(case when casetable.case_type like '%DV' then {0} end) as jims_case_dv_count_last_year, "
                      "count(case when casetable.case_type like '%JV' then {0} end) as jims_case_jv_count_last_year, "
                      "count(case when casetable.case_type like '%TC' then {0} end) as jims_case_tc_count_last_year, "
                      "count(case when casetable.case_type like '%TR' then {0} end) as jims_case_tr_count_last_year, "
                      "count(case when casetable.case_type like '%JC' then {0} end) as jims_case_jc_count_last_year, "
                      "count(case when casetable.case_type like '%CC' then {0} end) as jims_case_cc_count_last_year, "
                      "count(case when casetable.case_type like '%FG' then {0} end) as jims_case_fg_count_last_year "
                      "from {1} as casetable "
                      "where casetable.first_court_date < '{2}' and casetable.first_court_date > '{3}' "
                      "group by casetable.{0}").format(config_db['id_column'], config_db['jimscase'], self.fake_today, last_year_date )

class LastMonthCaseTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 3
        self.type_of_imputation = "zero"
        last_month_date =  self.fake_today - datetime.timedelta(days = 30)
        self.description = "Counts of case type: CR, DV and JV in the last year"
        self.query = ("select casetable.{0}, "
                      "count(case when casetable.case_type like '%CR' then {0} end) as jims_case_cr_count_last_month, "
                      "count(case when casetable.case_type like '%DV' then {0} end) as jims_case_dv_count_last_month, "
                      "count(case when casetable.case_type like '%JV' then {0} end) as jims_case_jv_count_last_month, "
                      "count(case when casetable.case_type like '%TC' then {0} end) as jims_case_tc_count_last_month, "
                      "count(case when casetable.case_type like '%TR' then {0} end) as jims_case_tr_count_last_month, "
                      "count(case when casetable.case_type like '%JC' then {0} end) as jims_case_jc_count_last_month, "
                      "count(case when casetable.case_type like '%CC' then {0} end) as jims_case_cc_count_last_month, "
                      "count(case when casetable.case_type like '%FG' then {0} end) as jims_case_fg_count_last_month "
                      "from {1} as casetable "
                      "where casetable.first_court_date < '{2}' and casetable.first_court_date > '{3}' "
                      "group by casetable.{0}").format(config_db['id_column'], config_db['jimscase'], self.fake_today, last_month_date )

class LastWeekCaseTypeCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 3
        self.type_of_imputation = "zero"
        last_week_date =  self.fake_today - datetime.timedelta(days = 7)
        self.description = "Counts of case type: CR, DV and JV in the last year"
        self.query = ("select casetable.{0}, "
                      "count(case when casetable.case_type like '%CR' then {0} end) as jims_case_cr_count_last_week, "
                      "count(case when casetable.case_type like '%DV' then {0} end) as jims_case_dv_count_last_week, "
                      "count(case when casetable.case_type like '%JV' then {0} end) as jims_case_jv_count_last_week, "
                      "count(case when casetable.case_type like '%TC' then {0} end) as jims_case_tc_count_last_week, "
                      "count(case when casetable.case_type like '%TR' then {0} end) as jims_case_tr_count_last_week, "
                      "count(case when casetable.case_type like '%JC' then {0} end) as jims_case_jc_count_last_week, "
                      "count(case when casetable.case_type like '%CC' then {0} end) as jims_case_cc_count_last_week, "
                      "count(case when casetable.case_type like '%FG' then {0} end) as jims_case_fg_count_last_week "
                      "from {1} as casetable "
                      "where casetable.first_court_date < '{2}' and casetable.first_court_date > '{3}' "
                      "group by casetable.{0}").format(config_db['id_column'], config_db['jimscase'], self.fake_today, last_week_date )

class ArrestingAgencyCount(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = "numerical"
        self.num_features = 4
        self.type_of_imputation = "zero"
        self.description = "Counts of arresting agency type:COUNTY SHERIFF, CITY, STATE and OTHER AGENCY"
        self.query = ("select booking.{0}, "
                      "count(case when booking.arr_agency like '%KS0460000' then {0} end) as jims_arresting_agency_ks0460000_count, "
                      "count(case when booking.arr_agency like '%KS0460600' then {0} end) as jims_arresting_agency_ks0460600_count, "
                      "count(case when booking.arr_agency like '%KS0460500' then {0} end) as jims_arresting_agency_ks0460500_count, "
                      "count(case when booking.arr_agency like '%KS0461000' then {0} end) as jims_arresting_agency_ks0461000_count, "
                      "count(case when booking.arr_agency like '%KS0460900' then {0} end) as jims_arresting_agency_ks0460900_count, "
                      "count(case when booking.arr_agency like '%KS0460300' then {0} end) as jims_arresting_agency_ks0460300_count, "
                      "count(case when booking.arr_agency like '%KS0460400' then {0} end) as jims_arresting_agency_ks0460400_count, "
                      "count(case when booking.arr_agency like '%KSKHP0100' then {0} end) as jims_arresting_agency_kskhp0100_count, "
                      "count(case when booking.arr_agency like '%KS0461200' then {0} end) as jims_arresting_agency_ks0461200_count, "
                      "count(case when booking.arr_agency like '%KS0460700' then {0} end) as jims_arresting_agency_ks0460700_count, "
                      "count(case when booking.arr_agency like '%KS0460200' then {0} end) as jims_arresting_agency_ks0460200_count, "
                      "count(case when booking.arr_agency like '%OTHER' then {0} end) as jims_arresting_agency_other_count, "
                      "count(case when booking.arr_agency like '%KS0460800' then {0} end) as jims_arresting_agency_ks0460800_count, "
                      "count(case when booking.arr_agency like '%KS0461700' then {0} end) as jims_arresting_agency_ks0461700_count, "
                      "count(case when booking.arr_agency like '%KS0461100' then {0} end) as jims_arresting_agency_ks0461100_count, "
                      "count(case when booking.arr_agency like '%KS0460100' then {0} end) as jims_arresting_agency_ks0460100_count "
                      "from {1} as booking  "
                      "where booking.bk_dt < '{2}' "
                      "group by booking.{0} ").format(config_db['id_column'], config_db['jimsbooking'], self.fake_today)

class CountOfJims(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self, **kwargs)
        self.type_of_features = 'numerical'
        self.description = "Number of JIMS visits between start_date to end_date"
        self.query = ("select booking.{0}, count(*) as jims_count "
                      "from {1} as booking "
                      "where booking.bk_dt < '{2}'  "
                      "group by booking.{0} ").format(config_db['id_column'], config_db['jimsbooking'], self.fake_today)
        self.type_of_imputation = "mean"

class LastWeekJimsCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Number of JIMS visits in the last week"
        last_week_date =  self.fake_today - datetime.timedelta(days = 7)
        self.query = ("select booking.{0}, count(booking.bk_dt) as last_week_jims_count "
                      "from {1} as booking "
                      "where booking.bk_dt < '{2}' and booking.bk_dt > '{3}' "
                      "group by booking.{0} ").format(config_db['id_column'], config_db['jimsbooking'], self.fake_today, last_week_date)
        self.type_of_imputation = "mean"

class LastMonthJimsCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Number of JIMS visits in the last month"
        last_week_date =  self.fake_today - datetime.timedelta(days = 30)
        self.query = ("select booking.{0}, count(booking.bk_dt) as last_month_jims_count "
                      "from {1} as booking "
                      "where booking.bk_dt < '{2}' and booking.bk_dt > '{3}' "
                      "group by booking.{0} ").format(config_db['id_column'], config_db['jimsbooking'], self.fake_today, last_week_date)
        self.type_of_imputation = "mean"

class LastYearJimsCount (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Number of JIMS visits in the last year"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select booking.{0}, count(booking.bk_dt) as last_year_jims_count "
                      "from {1} as booking "
                      "where booking.bk_dt < '{2}' and booking.bk_dt > '{3}' "
                      "group by booking.{0} ").format(config_db['id_column'], config_db['jimsbooking'], self.fake_today, last_year_date)
        self.type_of_imputation = "mean"

class LastYearJailDaysSum (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Total number of days in jail in past year"
        self.type_of_imputation = "mean"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select jail.{0}, "
                      "sum(end_date - begin_date) as jail_len_sum_year "
                      "from ( select booking.{0}, "
                      "case when rel_date < '{2}' then rel_date "
                      "else '{2}' "
                      "end as end_date, "
                      "case "
                      "when bk_dt > '{3}' then bk_dt "
                      "else '{3}' "
                      "end as begin_date, "
                      "rel_date, bk_dt "
                      "from {1} as booking "
                      "where bk_dt < '{2}' "
                      "and (rel_date> '{3}' or rel_date is null) "
                      ") as jail "
                      "group by jail.{0} ").format(config_db["id_column"], config_db['jimsbooking'], self.fake_today, last_year_date)

class LastYearJailDaysAvg (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Average number of days in jail in past year"
        self.type_of_imputation = "mean"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select jail.{0}, "
                      "avg(end_date - begin_date) as jail_len_avg_year "
                      "from ( select booking.{0}, "
                      "case when rel_date < '{2}' then rel_date "
                      "else '{2}' "
                      "end as end_date, "
                      "case "
                      "when bk_dt > '{3}' then bk_dt "
                      "else '{3}' "
                      "end as begin_date, "
                      "rel_date, bk_dt "
                      "from {1} as booking "
                      "where bk_dt < '{2}' "
                      "and (rel_date> '{3}' or rel_date is null) "
                      ") as jail "
                      "group by jail.{0} ").format(config_db["id_column"], config_db['jimsbooking'], self.fake_today, last_year_date)

class LastYearJailDaysStddev (abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Standard deviation of length of jail stays in past year"
        self.type_of_imputation = "mean"
        last_year_date =  self.fake_today - datetime.timedelta(days = 365)
        self.query = ("select jail.{0}, "
                      "stddev_samp(end_date - begin_date) as jail_len_stddev_year "
                      "from ( select booking.{0}, "
                      "case when rel_date < '{2}' then rel_date "
                      "else '{2}' "
                      "end as end_date, "
                      "case "
                      "when bk_dt > '{3}' then bk_dt "
                      "else '{3}' "
                      "end as begin_date, "
                      "rel_date, bk_dt "
                      "from {1} as booking "
                      "where bk_dt < '{2}' "
                      "and (rel_date> '{3}' or rel_date is null) "
                      ") as jail "
                      "group by jail.{0} ").format(config_db["id_column"], config_db['jimsbooking'], self.fake_today, last_year_date)

# Curent Charges

class CurrentChargesDrugOffense(abstract.SimpleFeature):
    def __init__(self):
        abstract.SimpleFeature.__init__(self)
        self.description = "Number of drug offense in current charges"
        self.query = ("select ccharge.personid, "
                      "count(case when ccharge.drug_offense = true then personid end) as count_drug_offense "
                      "from {} ccharge "
                      "group by ccharge.personid".format("premodeling.currentcharges_clean_table"))

class CurrentChargesFindingTrialOccurred(abstract.SimpleFeature):
    def __init__(self):
        abstract.SimpleFeature.__init__(self)
        self.description = "Number of trials occured in current charges"
        self.query = ("select ccharge.personid, "
                      "count(case when ccharge.trial_occurred = true then personid end) as count_trial_occurred)"
                      "from {} ccharge "
                      "group by ccharge.personid".format("premodeling.currentcharges_clean_table"))

class CurrentChargesFoundOrPleadGuilty(abstract.SimpleFeature):
    def __init__(self):
        abstract.SimpleFeature.__init__(self)
        self.description = "Number of a person found guilty in current charges"
        self.query = ("select ccharge.personid, "
                      "count(case when ccharge.found_or_plead_guilty = true then personid end) as count_found_guilty, "
                      "from {} ccharge "
                      "group by ccharge.personid".format("premodeling.currentcharges_clean_table"))

class CurrentChargesCoarseFinding(abstract.SimpleFeature):
    def __init__(self):
        abstract.SimpleFeature.__init__(self)
        self.type_of_feautes = "categorical histogram"
        self.num_features = 8
        self.description = "Histogram of coarse finding in current charges"
        self.query = ("select ccharge.personid, "
                      "count(case when ccharge.coarse_finding like '%GUILTY' then personid end) as guilty_count, "
                      "count(case when ccharge.coarse_finding like '%DISMISSED' then personid end) as dismissed_count, "
                      "count(case when ccharge.coarse_finding like '%OTHER TERMINATION' then personid end) as other_termination_count, "
                      "count(case when ccharge.coarse_finding like '%EXPUNGEMENT' then personid end) as expungement_count, "
                      "count(case when ccharge.coarse_finding like '%NOT GUILTY' then personid end) as not_guilty_count, "
                      "count(case when ccharge.coarse_finding like '%STAY ORDER' then personid end) as stay_order_count, "
                      "count(case when ccharge.coarse_finding like '%DIVERSION' then personid end) as diversion_count, "
                      "count(case when ccharge.coarse_finding like '%RELEASE FROM JURISDICTION' then personid end) as release_from_jurisdiction_count "
                      "from {} ccharge "
                      "group by ccharge.personid".format("premodeling.currentcharges_clean_table"))

class CurrentChargesFelonyOrMisdemeanor(abstract.SimpleFeature):
    def __init__(self):
        abstract.SimpleFeature.__init__(self)
        self.type_of_features = "categorical histogram"
        self.num_features = 2
        self.description = "Number of felony and misdemeanor in curent charges"
        self.query = ("select ccharge.personid, "
                      "count(case when ccharge.felony_or_misdemeanor like '%felony' then personid end) as felony_count, "
                      "count(case when ccharge.felony_or_misdemeanor like '%misdemeanor' then personid end) as misdemeanor_count "
                      "from {} ccharge "
                      "group by ccharge.personid".format("premodeling.currentcharges_clean_table"))

# Probation
class ProbationType(abstract.TimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.TimeBoundedFeature.__init__(self, **kwargs)
        self.type_of_features = "categorical histogram"
        self.num_features = 27
        self.description = "Histogram of probation type between start_date and end_date"
        self.query =()
