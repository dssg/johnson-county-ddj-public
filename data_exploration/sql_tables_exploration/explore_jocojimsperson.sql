/* This file conducts exploratory analyses in
 * preparation for cleaning the jocojimsperson
 * table and runs tests on the select statements
 * used in the query that creates the cleaned
 * view. */

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
    input.jocojimsperson
group by
    personid
order by
    count desc;

/* Some personids appear 2 times. Look at
 * full data for these ids. Note that each
 * is associated with 2 mni_nos. Only 1 
 * appears to have the same info */
select *
from
    input.jocojimsperson
where
    personid = 1080590 or
    personid = 1084872 or
    personid = 1086462 or
    personid = 1086844 or
    personid = 1088088 or
    personid = 1096003
order by
    personid;


/* Explore charges data for mni_nos
 * associated with personid 1086462 */
select *
from
    input.jocojimscurrentcharges
where
    mni_no = '000568568' or
    mni_no = '000362046'
order by
    mni_no;

/* Explore booking data for mni_nos
 * associated with personid 1086462 */
select *
from
    input.jocojimsjailbooking
where
    mni_no = '000568568' or
    mni_no = '000362046'
order by
    mni_no;

/* Explore previous bookings for mni_nos
 * associated with personid 1086462 */
select *
from
    input.jocojimspreviousbooking
where
    mni_no = '000568568' or
    mni_no = '000362046'
order by
    mni_no;

/* Look at the new data to see how
 * these mni_nos are represented.
 * Overall, very similar; two rows
 * per person */
select distinct
    *
from
    public.jocojimsperson
where
    mni_no in (
        select 
            mni_no
        from
            input.jocojimsperson
        where
            personid = 1080590 or
            personid = 1084872 or
            personid = 1086462 or
            personid = 1086844 or
            personid = 1088088 or
            personid = 1096003
    )
order by
    hash_ssn;

/* Verify that these represent the
 * extent of the duplicates in the
 * new data. */
select distinct
    hash_ssn,
    count(*)
from
    public.jocojimsperson
group by
    hash_ssn
order by
    count desc;

/* KDOC data contains more person
 * information, which might be
 * useful in deduping. */
select
    *
from
    input.kdoc_persons
where
    personid = 1080590 or
    personid = 1084872 or
    personid = 1086462 or
    personid = 1086844 or
    personid = 1088088 or
    personid = 1096003
order by
    personid;

/* Do these personids show up in our other
 * data sets?
 * Yes, two of them show up in mental health,
 * so corrections will need to apply to both */
select *
from
    premodeling.personid_event_dates
where
    personid = 1080590 or
    personid = 1084872 or
    personid = 1086462 or
    personid = 1086844 or
    personid = 1088088 or
    personid = 1096003
order by
    personid;

/* Look at the mental health data to identify
 * which mni_nos are likely matches. Fortunately,
 * the mental health dobs are exact matches to
 * exactly one of the mni_nos, so we can use
 * dobs to assist in the renumbering */
select
    *
from
    input.jocomentalhealth
where
    personid = 1084872 or
    personid = 1086844
order by
    personid;

/* Who are the people with no personids? */
select *
from
    input.jocojimsperson
where
    personid is null
order by
    dob;
    
/* What are the highest personids in
 * all the data? */
select
    personid
from
    premodeling.personid_event_dates
order by
    personid desc;
    
/* MNI_NO */

/* explore frequency distribution of
 * mni_nos */
select distinct
    mni_no,
    count(*)
from
    input.jocojimsperson
group by
    mni_no
order by
    count desc;
    
    
/* DOB */
    
/* How old were people at the start of
 * our data window? Sort by dob */
select distinct
    dob,
    age(timestamp '2010-01-01', dob) as age,
    count(*)
from
    input.jocojimsperson
group by
    dob
order by
    dob;
    
/* How old were people at the start of
 * our data window? Sort by frequency */
select distinct
    dob,
    age(timestamp '2010-01-01', dob) as age,
    count(*)
from
    input.jocojimsperson
group by
    dob
order by
    count desc;

/* Look at all the records for one of the most
 * common dobs */
select *
from
    input.jocojimsperson
where
    dob = timestamp '1988-04-15 00:00:00';
    

/* RACE */

/* list unique race values with counts */
select distinct
    race,
    count(*)
from
    input.jocojimsperson
group by
    race;

    
/* SEX */

/* list unique sex values with counts */
select distinct
    gender,
    count(*)
from
    input.jocojimsperson
group by
    gender;

    
/* CITY */
    
/* What cities are in data and how often?
 * Note that some cities are repeated in
 * sentence case and in upper case. */
select distinct
    city,
    count(*)
from
    input.jocojimsperson
group by
    city
order by
    city;


/* STATE */

/* List unique states with counts.
 * Note that ON is Ontario, Canada */
select distinct
    upper(state) as state,
    count(*)
from
    input.jocojimsperson
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
    input.jocojimsperson
group by
    zip
order by
    zip desc;

/* investigate JIMS data for individuals with
 * Canadian zip codes. First, look at person
 * information for these zips. */
select *
from
    input.jocojimsperson
where
    zip = 'N8W 2K9' or
    zip = 'L4Z' or
    zip = 'J0B';

/* Then look at charges for Canadian zips */
select *
from
    input.jocojimscurrentcharges
where
    mni_no in (
        select
            mni_no
        from
            input.jocojimsperson
        where
            zip = 'N8W 2K9' or
            zip = 'L4Z' or
            zip = 'J0B'
    )
order by
    mni_no;

/* Then look at bookings for Canadian zips */
select *
from
    input.jocojimsjailbooking
where
    mni_no in (
        select
            mni_no
        from
            input.jocojimsperson
        where
            zip = 'N8W 2K9' or
            zip = 'L4Z' or
            zip = 'J0B'
    )
order by
    mni_no;

/* Then look at pretrial for Canadian zips */
select *
from
    input.jocojimspretrialservice
where
    mni_no in (
        select
            mni_no
        from
            input.jocojimsperson
        where
            zip = 'N8W 2K9' or
            zip = 'L4Z' or
            zip = 'J0B'
    )
order by
    mni_no;
    
/* Then look at probation for Canadian zips */
select *
from
    input.jocojimsprobation
where
    mni_no in (
        select
            mni_no
        from
            input.jocojimsperson
        where
            zip = 'N8W 2K9' or
            zip = 'L4Z' or
            zip = 'J0B'
    )
order by
    mni_no;
    
    
/* BLOCK2010ID */
  
/* Examine frequencies block2010ids. */
select distinct
    blockgroup2010id,
    count(*)
from
    input.jocojimsperson
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
    input.jocojimsperson
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
    input.jocojimsperson
group by
    tract2010id
order by
    tract2010id desc;    



/* ********************************************
 *                   TESTS                    *
 * *******************************************/

/* Create duplicate personid column */
select distinct
    personid,
    case
        when personid in (
            select
                personid
            from (
                select
                    personid,
                    count(*) as idcount
                from
                    input.jocojimsperson
                group by
                    personid
                having
                    count(*) > 1
            ) as duplicate_ids
        ) then true
        else false
    end as duplicate_personid,
    count(*)
from
    input.jocojimsperson
group by
    personid
order by
    count desc;
    
    
/* Try converting race to ems race categories */
select
    upper(race) as original_race,
    case
      when race = 'A' then 'ASIAN'
      when race = 'B' then 'BLACK OR AFRICAN AMERICAN'
      when race = 'I' then 'AMERICAN INDIAN OR ALASKA NATIVE'
      when race = 'O' then 'OTHER RACE'
      when race = 'U' then null
      when race = 'W' then 'WHITE'
    end as race,
    count(*)
from input.jocojimsperson
group by
    race;

/* Replace empty string state with NULL */
select
    state as original_state,
    case
        when state = '' then null
        else state
    end as state,
    count(*)
from
    input.jocojimsperson
group by
    state
order by
    count desc;
    
/* Test converting zip codes to country,
 * first five, and last four */
select distinct
    case
        when zip = ' ' then null
        else zip
    end as zip,
    case
        when zip ~ '^(\d{5})' then 'UNITED STATES'
        when zip is null then null
        when zip = ' ' then null
        else 'OTHER'
    end as country,
    case
        when zip ~ '^(\d{5})' then split_part(zip, '-', 1)
    end as us_zip_first_five,
    case
        when zip ~ '^(\d{5})' and char_length(zip) > 5 then split_part(zip, '-', 2)
    end as us_zip_first_five
from
    input.jocojimsperson
order by
    zip desc;

    
    
    case
        when zip ~ '^(\d{5})' then 'UNITED STATES'
        when zip is null then null
        else 'OTHER'
    end as country