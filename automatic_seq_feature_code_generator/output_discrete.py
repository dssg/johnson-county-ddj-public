class BookingEmsBooking365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->ems exist for timeframe booking->ems '
        self.query=(" select distinct fin_table.{0}, 1 as booking_ems_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEms365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems exist for timeframe ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsMh365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->mh exist for timeframe ems->mh '
        self.query=(" select distinct fin_table.{0}, 1 as ems_mh_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhEms365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->ems exist for timeframe mh->ems '
        self.query=(" select distinct fin_table.{0}, 1 as mh_ems_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhMh365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->mh exist for timeframe mh->mh '
        self.query=(" select distinct fin_table.{0}, 1 as mh_mh_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEms365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems exist for timeframe ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsBooking365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->booking exist for timeframe ems->booking '
        self.query=(" select distinct fin_table.{0}, 1 as ems_booking_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingBooking365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->booking exist for timeframe booking->booking '
        self.query=(" select distinct fin_table.{0}, 1 as booking_booking_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEmsEms365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems->ems exist for timeframe ems->ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_ems_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhBooking365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->booking exist for timeframe mh->booking '
        self.query=(" select distinct fin_table.{0}, 1 as mh_booking_365_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 365) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEms182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems exist for timeframe ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsBooking182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->booking exist for timeframe ems->booking '
        self.query=(" select distinct fin_table.{0}, 1 as ems_booking_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingEms182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->ems exist for timeframe booking->ems '
        self.query=(" select distinct fin_table.{0}, 1 as booking_ems_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEms182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems exist for timeframe ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingBooking182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->booking exist for timeframe booking->booking '
        self.query=(" select distinct fin_table.{0}, 1 as booking_booking_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsMh182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->mh exist for timeframe ems->mh '
        self.query=(" select distinct fin_table.{0}, 1 as ems_mh_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEmsEms182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems->ems exist for timeframe ems->ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_ems_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingMh182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->mh exist for timeframe booking->mh '
        self.query=(" select distinct fin_table.{0}, 1 as booking_mh_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhMh182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->mh exist for timeframe mh->mh '
        self.query=(" select distinct fin_table.{0}, 1 as mh_mh_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhEms182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->ems exist for timeframe mh->ems '
        self.query=(" select distinct fin_table.{0}, 1 as mh_ems_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhBooking182 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->booking exist for timeframe mh->booking '
        self.query=(" select distinct fin_table.{0}, 1 as mh_booking_182_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 182) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEms90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems exist for timeframe ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingEms90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->ems exist for timeframe booking->ems '
        self.query=(" select distinct fin_table.{0}, 1 as booking_ems_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingBooking90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->booking exist for timeframe booking->booking '
        self.query=(" select distinct fin_table.{0}, 1 as booking_booking_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEms90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems exist for timeframe ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsMh90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->mh exist for timeframe ems->mh '
        self.query=(" select distinct fin_table.{0}, 1 as ems_mh_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEmsEms90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems->ems exist for timeframe ems->ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_ems_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhEms90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->ems exist for timeframe mh->ems '
        self.query=(" select distinct fin_table.{0}, 1 as mh_ems_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhBooking90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->booking exist for timeframe mh->booking '
        self.query=(" select distinct fin_table.{0}, 1 as mh_booking_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class MhMh90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence mh->mh exist for timeframe mh->mh '
        self.query=(" select distinct fin_table.{0}, 1 as mh_mh_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%mh->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsMh90 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->mh exist for timeframe ems->ems->mh '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_mh_90_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 90) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->mh%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEms30 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems exist for timeframe ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_30_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 30) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingEms30 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->ems exist for timeframe booking->ems '
        self.query=(" select distinct fin_table.{0}, 1 as booking_ems_30_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 30) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsBooking30 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->booking exist for timeframe ems->booking '
        self.query=(" select distinct fin_table.{0}, 1 as ems_booking_30_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 30) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->booking%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEmsEms30 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems->ems exist for timeframe ems->ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_ems_30_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 30) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class EmsEms7 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence ems->ems exist for timeframe ems->ems '
        self.query=(" select distinct fin_table.{0}, 1 as ems_ems_7_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 7) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)


class BookingEms7 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'TBD'
        self.description= 'does sequence booking->ems exist for timeframe booking->ems '
        self.query=(" select distinct fin_table.{0}, 1 as booking_ems_7_binary "
                        " from (select p3.{0}, t1.start_date, t1.end_date, string_agg(p3.event, '->') "
                        "from "
                                "(select t2.{0}, min(t2.start_date) as start_date, t2.end_date "
                                " from(select t.{0}, t.start_date, max(t.end_date) as end_date "
                                "from "
                                        "(select p1.{0}, p1.event as e1, p2.event as e2, p1.begin_date as start_date, p2.begin_date as end_date "
                                        "from {1} p1, {1} p2 "
                                        "where p1.{0} = p2.{0} and p1.begin_date <p2.begin_date and  "
                                        "p2.begin_date < '{2}' and p1.begin_date < '{2}' and "
                                        "date_part('days',p2.begin_date - p1.begin_date)  < 7) as t "
                                "group by t.{0}, t.start_date)as t2 "
                        "group by t2.{0}, t2.end_date) "
                        "as t1, {1} p3 "
        "where p3.begin_date > t1.start_date and p3.begin_date < t1.end_date and t1.{0} = p3.{0} "
        "group by p3.{0}, t1.start_date, t1.end_date "
        "order by p3.{0}, t1.start_date) as fin_table "
        "where string_agg like '%booking->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)
