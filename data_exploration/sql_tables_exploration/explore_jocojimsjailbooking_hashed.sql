/* This file conducts exploratory analyses in
 * preparation for cleaning the 
 * jocojimscurrentcharges table and runs tests
 * on the select statements used in the query
 * that creates the cleaned view. */

/* ********************************************
 *               EXPLORATIONS                 *
 * *******************************************/

/* MNI_NO */

/* Look at distribution of identifiers */
select distinct
    mni_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by
    mni_no
order by 
    count desc;
    

/* CASE_NO */

/* Look at distribution of case numbers */
select distinct
    case_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by
    case_no
order by 
    count desc;
    
/* Many case numbers result in multiple
 * bookings. Are these for seaparate 
 * individuals? */
select 
    *
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
where
    case_no = '14JV00718';


/* BOOKING_NO */

/* Look at distribution of booking numbers
 * Note that many occur twice */
select distinct
    booking_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by
    booking_no
order by 
    count desc;
    
/* Do all occur twice? */
select distinct
    booking_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by
    booking_no
order by 
    count;

/* Look at one of the duplicated
 * booking numbers. Note that it is
 * used for two people with different
 * case numbers and different booking
 * dates. */
select
    *
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
where
    booking_no = 10000005;


    
/* ARRESTING_AGENCY */
select distinct
	arresting_agency,
	count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by 
    arresting_agency
order by
    count;


/* BOOKING_DATE */
select distinct
	booking_date,
	count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by 
    booking_date
order by
    count desc;


/* RELEASE_DATE */
select distinct
	release_date,
	count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by 
    release_date
order by
    count desc;

/* BAIL_TYPE */ 

select distinct
	bail_type,
	count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by 
    bail_type
order by
    count desc;
    

/* BAIL_AMT */
    
select distinct
	bail_amt,
	count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by 
    bail_amt
order by
    count desc;


/* GEOM */
    
select distinct
	geom,
	count(*)
from (
    select distinct
        *
    from
        public.jocojimsjailbooking
) as bookings
group by 
    geom
order by
    count desc;


select DISTINCT
	arresting_agency,
	CASE
      WHEN arresting_agency similar to '%(CAMPUS|SCHOOL)%' THEN 'SCHOOL'
      WHEN arresting_agency similar to '%(SHERIFF)%' THEN 'COUNTY SHERIFF'
      WHEN arresting_agency similar to '(KANSAS)%' THEN 'STATE'
      WHEN arresting_agency similar to '(OTHER)%' THEN 'OTHER AGENCY'
      WHEN arresting_agency similar to '%(P.D.)' THEN 'CITY'
      WHEN arresting_agency similar to '%(PARK)%' THEN 'COUNTY PARK DISTRICT'
    END as felony_or_misdemeanor
from public.jocojimsjailbooking
order by arresting_agency;


