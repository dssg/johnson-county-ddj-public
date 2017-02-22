class BookingEmsBooking365 (abstract.TimeBoundedFeature):
    def __init__(self,**kwargs):
        abstract.TimeBoundedFeature.__init__(self,**kwargs)
        self.type_of_features = 'imputation zero'
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
        "where string_agg like '%ems->ems%' ").format(config_db['id_column'], config_db['personid_event_dates'], self.fake_today)
