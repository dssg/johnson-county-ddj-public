'''class CountOfEms(abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'numerical'
        self.description = "Number of EMS visits between start_date and end_date "
        self.query = ("select hash_ssn, count(*) as ems_count "
                      "from {0} ems "
                      "where incidentdate < '{1}' "
                      "group by hash_ssn ".format(self.db_tables['ems'],self.fake_today))
        self.type_of_imputation = "mean"'''

'''
select distinct fin_table.hash_ssn, 1
from (select p3.hash_ssn, t1.start_date, t1.end_date, string_agg(p3.event,'->')
  from
    (select t2.hash_ssn, min(t2.start_date) as start_date, t2.end_date
    from(select t.hash_ssn, t.start_date, max(t.end_date) as end_date
    from 
x      (select p1.hash_ssn, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date
      from premodeling.personid_event_dates_1 p1, premodeling.personid_event_dates_1 p2
      where p1.hash_ssn = p2.hash_ssn and p1.begin_date <p2.begin_date and 
x          date_part('days',p2.begin_date - p1.begin_date)  < 365) as t
    group by t.hash_ssn, t.start_date)as t2
    group by t2.hash_ssn, t2.end_date)
x    as t1, premodeling.personid_event_dates_1 p3
  where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.hash_ssn = p3.hash_ssn
  group by p3.hash_ssn, t1.start_date, t1.end_date
  order by p3.hash_ssn, t1.start_date) as fin_table
where string_agg like '%ems->ems%'
'''

def write_code(time_window, pattern, table_name):
    code = []
    code.append(("class {} (abstract.TimeBoundedFeature):").format(table_name))
    code.append("\t\tdef __init__(self,**kwargs):")
    code.append("\t\t\t\tabstract.TimeBoundedFeature.__init__(self,**kwargs)")
    code.append("\t\t\t\tself.type_of_features = 'TBD'")
    code.append(("\t\t\t\tself.description= \'does sequence {} exist for timeframe {} \'").format(pattern, pattern))
    code.append(("\t\t\t\tself.query=(\" select distinct fin_table.{0}, 1 as {1}_{2}_binary \"").format('{0}',pattern.replace('->', '_'), time_window ))
    code.append(("\t\t\t\t\t\t\" from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') \"").format("{0}"))
    code.append(("\t\t\t\t\t\t\"from \"").format())
    code.append(("\t\t\t\t\t\t\t\"(select t2.{0}, min(t2.start_date) as start_date, t2.end_date \"").format('{0}'))
    code.append(("\t\t\t\t\t\t\t\" from(select t.{0}, t.start_date, max(t.end_date) as end_date \"").format('{0}'))
    code.append(("\t\t\t\t\t\t\t\"from \"").format())
    code.append(("\t\t\t\t\t\t\t\t\"(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date \"").format('{0}'))
    code.append(("\t\t\t\t\t\t\t\t\"from {0} p1, {0} p2 \"").format('{1}'))
    code.append(("\t\t\t\t\t\t\t\t\"where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  \"").format('{0}'))
    code.append(("\t\t\t\t\t\t\t\t\"p2.begin_date < '{0}' and p1.begin_date < '{0}' and \"").format('{2}'))
    code.append(("\t\t\t\t\t\t\t\t\"date_part('days',p2.begin_date - p1.begin_date)  < {0}) as t \"").format(time_window))
    code.append(("\t\t\t\t\t\t\t\"group by t.{0}, t.start_date)as t2 \"").format('{0}'))
    code.append(("\t\t\t\t\t\t\"group by t2.{0}, t2.end_date) \"").format('{0}'))
    code.append(("\t\t\t\t\t\t\"as t1, {0} p3 \"").format('{1}'))
    code.append(("\t\t\t\t\"where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} \"").format('{0}'))
    code.append(("\t\t\t\t\"group by p3.{0}, t1.start_date, t1.end_date \"").format('{0}'))
    code.append(("\t\t\t\t\"order by p3.{0}, t1.start_date) as fin_table \"").format('{0}'))
    code.append(("\t\t\t\t\"where string_agg like '%{0}%' \").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)").format(pattern))
    return code
  
  
features_to_gen = []
features_to_gen.append({'pattern': 'booking->ems', 'class_name': 'BookingEmsBooking365', 'time_frame': 365, 'yaml_name': 'booking_ems_365'})
features_to_gen.append({'pattern': 'ems->ems->ems', 'class_name': 'EmsEmsEms365', 'time_frame': 365, 'yaml_name': 'ems_ems_ems_365'})
features_to_gen.append({'pattern': 'ems->mh', 'class_name': 'EmsMh365', 'time_frame': 365, 'yaml_name': 'ems_mh_365'})
features_to_gen.append({'pattern': 'mh->ems', 'class_name': 'MhEms365', 'time_frame': 365, 'yaml_name': 'mh_ems_365'})
features_to_gen.append({'pattern': 'mh->mh', 'class_name': 'MhMh365', 'time_frame': 365, 'yaml_name': 'mh_mh_365'})
features_to_gen.append({'pattern': 'ems->ems', 'class_name': 'EmsEms365', 'time_frame': 365, 'yaml_name': 'ems_ems_365'})
features_to_gen.append({'pattern': 'ems->booking', 'class_name': 'EmsBooking365', 'time_frame': 365, 'yaml_name': 'ems_booking_365'})
features_to_gen.append({'pattern': 'booking->booking', 'class_name': 'BookingBooking365', 'time_frame': 365, 'yaml_name': 'booking_booking_365'})
features_to_gen.append({'pattern': 'ems->ems->ems->ems', 'class_name': 'EmsEmsEmsEms365', 'time_frame': 365, 'yaml_name': 'ems_ems_ems_ems_365'})
features_to_gen.append({'pattern': 'mh->booking', 'class_name': 'MhBooking365', 'time_frame': 365, 'yaml_name': 'mh_booking_365'})



features_to_gen.append({'pattern': 'ems->ems', 'class_name': 'EmsEms182', 'time_frame': 182, 'yaml_name': 'ems_ems_182'})
features_to_gen.append({'pattern': 'ems->booking', 'class_name': 'EmsBooking182', 'time_frame': 182, 'yaml_name': 'ems_booking_182'})
features_to_gen.append({'pattern': 'booking->ems', 'class_name': 'BookingEms182', 'time_frame': 182, 'yaml_name': 'booking_ems_182'})
features_to_gen.append({'pattern': 'ems->ems->ems', 'class_name': 'EmsEmsEms182', 'time_frame': 182, 'yaml_name': 'ems_ems_ems_182'})
features_to_gen.append({'pattern': 'booking->booking', 'class_name': 'BookingBooking182', 'time_frame': 182, 'yaml_name': 'booking_booking_182'})
features_to_gen.append({'pattern': 'ems->mh', 'class_name': 'EmsMh182', 'time_frame': 182, 'yaml_name': 'ems_mh_182'})
features_to_gen.append({'pattern': 'ems->ems->ems->ems', 'class_name': 'EmsEmsEmsEms182', 'time_frame': 182, 'yaml_name': 'ems_ems_ems_ems_182'})
features_to_gen.append({'pattern': 'booking->mh', 'class_name': 'BookingMh182', 'time_frame': 182, 'yaml_name': 'booking_mh_182'})
features_to_gen.append({'pattern': 'mh->mh', 'class_name': 'MhMh182', 'time_frame': 182, 'yaml_name': 'mh_mh_182'})
features_to_gen.append({'pattern': 'mh->ems', 'class_name': 'MhEms182', 'time_frame': 182, 'yaml_name': 'mh_ems_182'})
features_to_gen.append({'pattern': 'mh->booking', 'class_name': 'MhBooking182', 'time_frame': 182, 'yaml_name': 'mh_booking_182'})

features_to_gen.append({'pattern': 'ems->ems', 'class_name': 'EmsEms90', 'time_frame': 90, 'yaml_name': 'ems_ems_90'})
features_to_gen.append({'pattern': 'booking->ems', 'class_name': 'BookingEms90', 'time_frame': 90, 'yaml_name': 'booking_ems_90'})
features_to_gen.append({'pattern': 'booking->booking', 'class_name': 'BookingBooking90', 'time_frame': 90, 'yaml_name': 'booking_booking_90'})
features_to_gen.append({'pattern': 'ems->ems->ems', 'class_name': 'EmsEmsEms90', 'time_frame': 90, 'yaml_name': 'ems_ems_ems_90'})
features_to_gen.append({'pattern': 'ems->mh', 'class_name': 'EmsMh90', 'time_frame': 90, 'yaml_name': 'ems_mh_90'})
features_to_gen.append({'pattern': 'ems->ems->ems->ems', 'class_name': 'EmsEmsEmsEms90', 'time_frame': 90, 'yaml_name': 'ems_ems_ems_ems_90'})
features_to_gen.append({'pattern': 'mh->ems', 'class_name': 'MhEms90', 'time_frame': 90, 'yaml_name': 'mh_ems_90'})
features_to_gen.append({'pattern': 'mh->booking', 'class_name': 'MhBooking90', 'time_frame': 90, 'yaml_name': 'mh_booking_90'})
features_to_gen.append({'pattern': 'mh->mh', 'class_name': 'MhMh90', 'time_frame': 90, 'yaml_name': 'mh_mh_90'})
features_to_gen.append({'pattern': 'ems->ems->mh', 'class_name': 'EmsEmsMh90', 'time_frame': 90, 'yaml_name': 'ems_ems_mh_90'})

features_to_gen.append({'pattern': 'ems->ems', 'class_name': 'EmsEms30', 'time_frame': 30, 'yaml_name': 'ems_ems_30'})
features_to_gen.append({'pattern': 'booking->ems', 'class_name': 'BookingEms30', 'time_frame': 30, 'yaml_name': 'booking_ems_30'})
features_to_gen.append({'pattern': 'ems->booking', 'class_name': 'EmsBooking30', 'time_frame': 30 ,'yaml_name': 'ems_booking_30'})
features_to_gen.append({'pattern': 'ems->ems->ems', 'class_name': 'EmsEmsEms30', 'time_frame': 30, 'yaml_name': 'ems_ems_ems_30'})

features_to_gen.append({'pattern': 'ems->ems', 'class_name': 'EmsEms7', 'time_frame': 7, 'yaml_name': 'ems_ems_7'})
features_to_gen.append({'pattern': 'booking->ems', 'class_name': 'BookingEms7', 'time_frame': 7, 'yaml_name': 'booking_ems_7'})

#   'age_first_interaction_public_service_years': person.AgeFirstInteractionPublicServiceInYears(**kwargs),
def dict_call(call_name, method_name):
    str_ = ("'{0}': seqfeatures.{1}(**kwargs), ").format(call_name, method_name)
    return str_
  
  
code_lines = []
dict_lines = []
class_map = []
for feature in features_to_gen:
    dict_lines.append(("{}: True").format(feature['yaml_name']))
    lst = write_code(feature['time_frame'], feature['pattern'], feature['class_name'])
    code_lines.extend(lst)
    code_lines.append("")
    code_lines.append("")
    class_map.append(dict_call(feature['yaml_name'], feature['class_name']))
  
  
  
with open('output_discrete.py', 'w') as f:
    for s in code_lines:
        f.write(s + '\n')

with open('class_map.py', 'w') as f:
    for s in class_map:
        f.write(s + '\n')

with open('dict.py', 'w') as f:
    for s in dict_lines:
        f.write(s + '\n')

