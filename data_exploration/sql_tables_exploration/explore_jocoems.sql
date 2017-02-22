/* This file conducts exploratory analyses in
 * preparation for cleaning the jocoems table
 * and runs tests on the select statements
 * used in the view query that creates the
 * cleaned view. */

/* ********************************************
 *               EXPLORATIONS                 *
 * *******************************************/

/* PERSONID */

/* explore frequency distribution of
 * personids */
select distinct
    personid,
    count(*)
from
    input.jocoems
group by
    personid
order by
    count desc;

    
/* DOB */
    
/* How old were people at the start of
 * our data window? Note lots of people
 * born 1900-01-01. */
select distinct
    dob,
    age(timestamp '2010-01-01', dob) as age,
    count(*)
from
    input.jocoems
group by
    dob,
order by
    dob;

/* How many runs per person, ordered by dob? */
select distinct
    dob,
    personid,
    age(timestamp '2010-01-01', dob) as age,
    count(*)
from
    input.jocoems
group by
    dob,
    personid
order by
    dob;

/* Examine EMS calls for people with 1900-01-01  dob.
 * Start with person who has 73  seaparate calls */
select *
from
    input.jocoems
where
    personid = 1038999
order by
    incidentdate;
    
/* Now someone who has three calls.
 * Note that two of the three have the same
 * values in all columns. */
select *
from
    input.jocoems
where
    personid = 1014006
order by
    incidentdate;
    
/* Another 1900-01-01 dob person with
 * two entries with the same exact info */
select *
from
    input.jocoems
where
    personid = 1031432
order by
    incidentdate;

    
select *
from
    input.jocoems
where
    personid = 1029146
order by
    incidentdate;
    

/* RACE */

/* list unique race values with counts */
select distinct
    race,
    count(*)
from
    input.jocoems
group by
    race;

    
/* SEX */

/* list unique sex values with counts */
select distinct
    sex,
    count(*)
from
    input.jocoems
group by
    sex;

    
/* CITY */
    
/* What cities are in data and how often?
 * Note that some cities are repeated in
 * sentence case and in upper case. */
select distinct
    upper(city) as city,
    count(*)
from
    input.jocoems
group by
    city
order by
    city;


/* STATE */

/* List unique states with counts */
select distinct
    upper(state) as state,
    count(*)
from
    input.jocoems
group by
    state
order by
    count desc;
    

/* ZIP */

/* List unique zip codes and counts.
 * Note that some zip codes inlcude
 * last four but not all. Also, two
 * Canadian zip codes in data */
select distinct
    zip,
    count(*)
from
    input.jocoems
group by
    zip
order by
    zip desc;

/* investigate EMS runs for Canadian zip codes.
 * First, look at actual runs for these zips. */
select *
from
    input.jocoems
where
    zip = 'V2M';

select *
from
    input.jocoems
where
    zip = 'K1E';
    
/* Next, look at whether additional runs exist
 * for the same individuals. Only one actually
 * has a personid. */
select *
from
    input.jocoems
where
    personid = 1062969;


/* BLOCK2010ID */
  
/* Examine frequencies block2010ids. */
select distinct
    blockgroup2010id,
    count(*)
from
    input.jocoems
group by
    blockgroup2010id
order by
    blockgroup2010id desc;


/* BLOCKGROUP2010ID */
  
/* Examine frequencies blockgroup2010ids. */
select distinct
    block2010id,
    count(*)
from
    input.jocoems
group by
    block2010id
order by
    block2010id desc;


/* TRACT2010ID */

/* Examine frequency distribution for 
 * tract2010ids. */
select distinct
    tract2010id,
    count(*)
from
    input.jocoems
group by
    tract2010id
order by
    tract2010id desc;    

/* How many unique tracts are in joco? */
select count(*)
    geoid10
from 
    input.jocoblock2010_pl;


/* INCIDENTDATE */

/* Look at frequencies for incident dates.
 * Note that many dates have 00:00:00 for
 * the time. These make up the bulk of the
 * repeat dates. However, there are some
 * dates with exact times that repeat
 * several times. */
select distinct
    incidentdate,
    count(*)
from
    input.jocoems
group by
    incidentdate
order by
    count desc;

/* How many calls occur on different days,
 * regardless of time? */
select distinct
    date(incidentdate),
    count(*)
from
    input.jocoems
group by
    date
order by
    count desc;
    
    
/* look closely at some dates with many incidents.
 * Note that when there are many calls for the same
 * exact time, most people refuse care. What kinds
 * of events do these represent? */
select *
from
    input.jocoems
where
    incidentdate = timestamp '2011-10-22 07:56:31';

/* look closely at some dates with many incidents.
 * Note that when there are many calls for the same
 * exact time, most people refuse care. What kinds
 * of events do these represent? */
select *
from
    input.jocoems
where
    incidentdate = timestamp '2013-12-03 12:15:30';

/* look closely at some dates with many incidents.
 * Note that when there are many calls for the same
 * exact time, most people refuse care. What kinds
 * of events do these represent? */
select *
from
    input.jocoems
where
    incidentdate = timestamp '2011-12-13 00:00:00'
order by
	personid;

/* How many incidentdates have
 * 00:00:00 for time? */
select distinct
    "time"(incidentdate),
    count(*)
from
    input.jocoems
group by
    time
order by
    count desc;


/* PRIMARYIMPRESSION */

/* What values are used for primary
 * impressions and their frequencies? */
select distinct
    primaryimpression,
    count(*)
from
    input.jocoems
group by
    primaryimpression
order by
    count desc;

/* Look at triage code for 'obvious death' cases.
 * Note that sometimes green triage codes occur
 * for 'obvious death.' Why? */
select 
    primaryimpression,
    triage
from
    input.jocoems
where
    primaryimpression similar to 'Obvious Death';

/* Look at EMS calls that have 'Not Applicable'
 * primary impressions. Note that most refer to
 * situations where no one was treated, many
 * still involve treating and transporting a
 * patient. */
select *
from
    input.jocoems
where
    primaryimpression = 'Not Applicable'
order by
    disposition;


/* TRIAGE */
    
/* What values are used for triage codes? */ 
select distinct
    triage
from
    input.jocoems
order by
    triage;

/* How many blank triage codes? */
select count(*)
    triage
from
    input.jocoems
where
    triage = '';

/* EMS contact on call said that a triage code
 * will not be entered when there is no patient.
 * Is this generally true in our data?
 * Yes, but note that there are still many
 * cases where a triage code is entered
 * anyway. */
select
    triage,
    disposition,
    count(*)
from
    input.jocoems
where
    disposition = 'No Patient'
group by
    triage,
    disposition
order by
    count desc;


/* DISPOSITION */

/* What values are used for disposition,
 * and what are their frequencies? */
select distinct
    disposition,
    count(*)
from
    input.jocoems
group by
    disposition
order by
    count desc;
    
/* Look at rows with 'N/A' or 'Not Applicable'
 * dispositions */
select *
from 
    input.jocoems
where
    disposition = 'Not Applicable';
    
select *
from 
    input.jocoems
where
    disposition = 'N/A';


/* ********************************************
 *                   TESTS                    *
 * *******************************************/

/* Try converting several null race values to NULLs */
select
    case 
        when upper(race) similar to '(NOT )%' then null
        when upper(race) = '' then null
        else upper(race)
    end as race,
    count(*)
from input.jocoems
group by
    race;

/* Test trimming extraneous spaces from triage
 * data and converting to uppercase. */
select distinct
    triage,
    upper(trim(both ' ' from triage))
from
    input.jocoems;

/* Test converting city to uppercase. */
select distinct
    city,
    upper(city)
from
    input.jocoems
order by
    city;
    
/* Test code to generate joco_resident
 * column based on tract2010id */
select
    block2010id,
    exists (select * 
        from input.jocoblock2010_pl
        where geoid10 = block2010id)
    as joco_resident
from
    input.jocoems;

/* Are all given blockids in joco?
 * Yes. */
select distinct
    exists (select * 
        from input.jocoblock2010_pl
        where geoid10 = block2010id)
    as joco_resident
from
    input.jocoems
where
    block2010id is not null
order by
    joco_resident;

/* Test selecting distinct rows for someone
 * known to have have 2 dinstinct rows and
 * 1 duplicate row. Should return 2 distinct
 * rows.
 */ 
select distinct *
from
    input.jocoems
where
    personid = 1014006
order by
    incidentdate;

/* How many rows are in the original data?
 * 196,981 */
select
    count(*)
from
    input.jocoems;

/* And how many are distinct?
 * 199,916 */
select
    count(*)
from
    (select distinct * 
    from 
        input.jocoems)
    as distinct_rows;

/* Try cleaning dummy dobs */
select
    case
        when dob = timestamp '1900-01-01 00:00:00' and personid is null then null
        else dob
    end as dob_clean,
    count(*)
from input.jocoems
group by
    dob_clean;

/* Try cleaning sex */
select
    case
        when upper(sex) = 'UNKNOWN' then null
        else upper(sex)
    end as sex,
    count(*)
from input.jocoems
group by
    sex;

/* Try splitting only US zips into two columns. */
select
    zip,
    case
        when char_length(zip) > 4 then split_part(zip, '-', 1)
    end as us_zip_first_five,
    case
        when char_length(zip) > 5 then split_part(zip, '-', 2)
    end as us_zip_last_four,
    count(*)
from input.jocoems
group by
    zip,
    us_zip_first_five,
    us_zip_last_four
order by
    zip desc;

/* Try making transported column */
select
    disposition,
    case
        when upper(disposition) similar to '%(TRANSPORTED)%' then true
        when disposition is null then null
        else false
    end as transported,
    count(*)
from input.jocoems
group by
    disposition,
    transported;

/* try separating just the date from incidentdate. */
select
    date(incidentdate)
from
    input.jocoems;
