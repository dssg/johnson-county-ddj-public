/* This file conducts exploratory analyses in
 * preparation for cleaning the jocoems table
 * and runs tests on the select statements
 * used in the view query that creates the
 * cleaned view. */

/* ********************************************
 *               EXPLORATIONS                 *
 * *******************************************/

/* HASH_SSN */

/* explore frequency distribution of
 * SSNs */
select distinct
    hash_ssn,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
    ) as ems
group by
    hash_ssn
order by
    count desc;
    
/* The numbers of calls for the most frequent callers
 * have changed since the previous data. Look at the
 * calls to investigate why. */
select
    *
from (
    select distinct
        *
    from
        input.jocoems
) as ems
where
    personid = 1015921
order by
    incidentdate desc;
    
select
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_ssn = E'\\x53662052026BC74B0FF3D9B5AAEB7CE0D955953F8615B8C1FEA57D29B54704FB'
order by
    incidentdate desc;
    
--84
select
    *
from (
    select distinct
        *
    from
        input.jocoems
) as ems
where
    personid = 1004724
order by
    incidentdate desc;
--89
select
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_ssn = E'\\x1BD02BF773CA742D9E2D08E370EB23FA402B46A69A2CE36747DCBA165B5B5A34'
order by
    incidentdate desc;
    
--89
select
    *
from (
    select distinct
        *
    from
        input.jocoems
) as ems
where
    personid = 1064984
order by
    incidentdate desc;    
--90    
select
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_ssn = E'\\x757C75060B0E960D647B4EC68E1D4C91BAC28BD7A48D60CAA1E279AA65214B5E'
order by
    incidentdate desc;


    
--83  
select
    *
from (
    select distinct
        *
    from
        input.jocoems
) as ems
where
    personid = 1046859
order by
    incidentdate desc; 
--85  
select
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_ssn = E'\\xD769CEAA748FA1F8CF07117023CBB378C9D2565DB3D83303876F9A2998A239F5'
order by
    incidentdate desc;
    
--83
select
    *
from (
    select distinct
        *
    from
        input.jocoems
) as ems
where
    personid = 1079017
order by
    incidentdate desc; 
--86
select
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_ssn = E'\\xD488CEB24F952D12CF3B0BC5B1E8679997E62AF8E76B2206AE37207FD2886940'
order by
    incidentdate desc;
    
/* DOB */
    
/* How old were people at the start of
 * our data window? Note lots of people
 * born 1900-01-01. */
select distinct
    dob,
    age(timestamp '2010-01-01', dob) as age,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    dob
order by
    dob;


/* How many runs per person, ordered by dob? */
select distinct
    dob,
    hash_ssn,
    age(timestamp '2010-01-01', dob) as age,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    dob,
    hash_ssn
order by
    dob;

/* Examine EMS calls for people with 1900-01-01
 * dob. */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    dob = timestamp '1900-01-01 00:00:00'
order by
    incidentdate;
    
/* Examine EMS calls for people with 1901-01-01
 * dob. */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    dob = timestamp '1901-01-01 00:00:00'
order by
    incidentdate;
    

/* RACE */

/* list unique race values with counts */
select distinct
    race,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    race;

    
/* SEX */

/* list unique sex values with counts */
select distinct
    sex,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    sex;
    

/* HOMELESS */

/* list unique homeless values with counts */
select distinct
    homeless,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    homeless;
    
/* how does homeless co-occur with hash_ssn? */
select distinct
    homeless,
    hash_ssn,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    homeless,
    hash_ssn
order by
    count desc;
    
/* how does homeless co-occur with hash_ssn? */
select distinct
    ems.hash_ssn,
    exists (
        select
            *
        from
            public.jocoems
        where
            hash_ssn = ems.hash_ssn and
            homeless = 1
    ) as ever_homeless,    
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    hash_ssn,
    ever_homeless
order by
    count desc;

/* What are the base rates for people to
 * ever be flagged homeless? */
select distinct
    ever_homeless,    
    count(*)
from (
    select distinct
    ems.hash_ssn,
    exists (
        select
            *
        from
            public.jocoems
        where
            hash_ssn = ems.hash_ssn and
            homeless = 1
    ) as ever_homeless,    
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
    ) as ems
    group by
        hash_ssn,
        ever_homeless
) as homeless_list
group by
    ever_homeless
order by
    count desc;
    
/* RESCITY */
    
/* What cities are in data and how often?
 * Note that some cities are repeated in
 * sentence case and in upper case. */
select distinct
    upper(rescity) as rescity,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    rescity
order by
    rescity;


/* RESSTATE */

/* List unique states with counts */
select distinct
    upper(resstate) as resstate,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    resstate
order by
    count desc;
    

/* RESZIP */

/* List unique zip codes and counts.
 * Note that some zip codes inlcude
 * last four but not all. Also, two
 * Canadian zip codes in data */
select distinct
    reszip,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    reszip
order by
    reszip desc;

/* investigate EMS runs for Canadian zip codes.
 * First, look at actual runs for these zips. */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    reszip = 'V2M' or
    reszip = 'K1E';
    
/* Next, look at whether additional runs exist
 * for the same individuals. Only one actually
 * has a hashed ssn. */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_ssn = E'\\xCE9081967FDB399318AF10DE39A56052FA3EE24B711BE0A11CF547CD28D1C8ED';
    
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    hash_dob_sex_lname_fname = E'\\xABF2416281ABA8F202EB7001795C2317AA1DEEA3BF6A0B8750C43B44C263437F' or
    hash_dob_lname_fname = E'x8F14D9EEAC1AD242F26F799336E1D6427E5AF0899B098595B5B1A52C8C57945E' or
    hash_dob_sex_lname = E'x5F77E629B1A8EED4607790ACD5C73FB54465B361B9325241E99615F286DA03E3' or
    hash_dob_lname = E'x1C49AD67B0C902B77B904234930F0E2A4D5236704099F86EB46C8787B28F4FC2';


/* RESBLOCK2010ID */
  
/* Examine frequencies residence block2010ids. */
select distinct
    resblockgroup2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    resblockgroup2010id
order by
    resblockgroup2010id desc;


/* RESBLOCKGROUP2010ID */
  
/* Examine frequencies residence blockgroup2010ids. */
select distinct
    resblock2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    resblock2010id
order by
    resblock2010id desc;


/* RESTRACT2010ID */

/* Examine frequency distribution for 
 * residence tract2010ids. */
select distinct
    restract2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    restract2010id
order by
    restract2010id desc;

    
/* CALLCITY */
    
/* What call cities are in data and how
 * often? Note that some cities are repeated
 * in sentence case and in upper case. */
select distinct
    upper(callcity) as callcity,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    callcity
order by
    callcity;


/* CALLSTATE */

/* List unique call states with counts */
select distinct
    upper(callstate) as callstate,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    callstate
order by
    count desc;
    
/* Lots of calls to MO. Why? */
select 
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    callstate = 'MO'
order by
    callcity;
    

/* CALLZIP */

/* List unique call zip codes and
 * counts. Note that some zip codes
 * inlcude last four but not all.
 * Also, several malformed zip codes.
 * Will need to extract first string
 * of five contiguous digits. */
select distinct
    callzip,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    callzip
order by
    callzip desc;

/* investigate EMS runs for malformed zip codes.
 * First, look at actual runs for these zips. */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    callzip = 'HUSBA66204' or
    callzip = 'C66062' or
    callzip = '3242' or
    callzip = '2 PT66062' or
    callzip = '0.66062' or
    callzip = '0'
order by
    callzip;


/* CALLBLOCK2010ID */
  
/* Examine frequencies call block2010ids. */
select distinct
    callblockgroup2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    callblockgroup2010id
order by
    callblockgroup2010id desc;
    
/* 7000 calls without call block groups. */
select 
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    callblockgroup2010id is null
order by
    hash_ssn;
    

/* CALLBLOCKGROUP2010ID */
  
/* Examine frequencies call blockgroup2010ids. */
select distinct
    callblock2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    callblock2010id
order by
    callblock2010id desc;


/* CALLTRACT2010ID */

/* Examine frequency distribution for 
 * call tract2010ids. */
select distinct
    calltract2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    calltract2010id
order by
    calltract2010id desc;


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
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    incidentdate
order by
    count desc;

/* How many calls occur on different days,
 * regardless of time? */
select distinct
    date(incidentdate),
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    date
order by
    count desc;


/* How many incidentdates have
 * 00:00:00 for time? */
select distinct
    "time"(incidentdate),
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    time
order by
    count desc;
    
/* Compare to original data */
select distinct
    "time"(incidentdate),
    count(*)
from (
    select distinct
        *
    from
        input.jocoems
) as ems
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
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    primaryimpression
order by
    count desc;

/* Look at triage code for 'obvious death' cases.
 * Note that sometimes green triage codes occur
 * for 'obvious death.' Why? */
select 
    primaryimpression,
    trim(both ' ' from triage) as triage,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    primaryimpression similar to 'Obvious Death'
group by
    primaryimpression,
    triage;

/* Look at EMS calls that have 'Not Applicable'
 * primary impressions. Note that most refer to
 * situations where no one was treated, many
 * still involve treating and transporting a
 * patient. */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    primaryimpression = 'Not Applicable'
order by
    disposition;


/* TRIAGE */
    
/* What values are used for triage codes? */ 
select distinct
    triage
from (
    select distinct
        *
    from
        public.jocoems
) as ems
order by
    triage;

/* How many blank triage codes? */
select count(*)
    triage
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    triage is null;

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
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    disposition = 'No Patient'
group by
    triage,
    disposition
order by
    count desc;

/* But this only accounts for about 1/4
 * of blank triage codes. What are the
 * dispositions/impressions for the blank
 * triage codes like?
 * Null primary impressions make up about
 * half of the blank triage codes, with
 * no apparent illness/injury being the
 * next largest category. */
select
    triage,
    primaryimpression,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    triage is null
group by
    triage,
    primaryimpression
order by
    count desc;
    
/* No patient, patient refused care, or
 * no treatment required make up the
 * bulk of the blank triage codes, but
 * about a a quarter were treated. */   
select
    triage,
    disposition,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    triage is null
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
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    disposition
order by
    count desc;
    
/* Look at rows with 'N/A' or 'Not Applicable'
 * dispositions */
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    disposition = 'Not Applicable'
order by
    triage,
    primaryimpression,
    destination;
    
select *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    disposition = 'N/A'
order by
    triage,
    primaryimpression,
    destination;
    
    
/* DESTINATION */

/* What values are used for disposition,
 * and what are their frequencies? */
select distinct
    destination,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    destination
order by
    destination;

/* Where do people who transfer care go? */
select distinct
    destination,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    disposition = 'Treated  Transferred Care'
group by
    destination
order by
    destination;
    
    
/* Where do people transported by LE go? */
select distinct
    destination,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    disposition = 'Treated  Transported by Law Enforcement'
group by
    destination
order by
    destination;
    
/* Look at calls where people go to RSI */
select 
    *
from (
    select distinct
        *
    from
        public.jocoems
) as ems
where
    destination = 'Rainbow Treatment Facilities'
order by
    primaryimpression;

/* Potential null values:
 * -Not Applicable
 * -Not Known
 */
    
    
/* HASH_SERVICEPROVIDERNAME */

/* What values are used for serive provider,
 * names and what are their frequencies? */
select distinct
    hash_serviceprovidername,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    hash_serviceprovidername
order by
    count desc;

/* How many unique values? */
select
    count(*)
from (
    select distinct
        hash_serviceprovidername
    from (
        select distinct
            *
        from
            public.jocoems
    ) as ems
) as service_providers;


/* GEOM */

/* What values are used for serive provider,
 * names and what are their frequencies? */
select distinct
    geom,
    count(*)
from (
    select distinct
        *
    from
        public.jocoems
) as ems
group by
    geom
order by
    count desc;

    

/* ********************************************
 *                   TESTS                    *
 * *******************************************/

