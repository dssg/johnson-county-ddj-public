/* This file conducts exploratory analyses in
 * preparation for cleaning the jocojimsperson
 * table and runs tests on the select statements
 * used in the query that creates the cleaned
 * view. */

/* ********************************************
 *               EXPLORATIONS                 *
 * *******************************************/

/* HASH_SSN */

/* explore frequency distribution of SSNs */
select distinct
    hash_ssn,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    hash_ssn
order by
    count desc;

/* Some SSNs appear 2 times. Look at full data
 * for these ids. Note that each is associated
 * with 2 mni_nos. Only 1 appears to have the
 * same info, but after examining KDOC files,
 * looks to be twins with erroneously
 * identical SSNs. See exploration script for
 * unhashed original data. */
select
    *
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
where
    hash_ssn = E'\\x3B3291CCDE0FEFD5435173DF9CF1865E4D11CD8EE9AF8FD74B89085EE515B7D4' or
    hash_ssn = E'\\x3DDC445E1057C9E1E00D74261DC0078F4A42BC1980FB0E9EA3F02A7A648AFAB9' or
    hash_ssn = E'\\x529360592E153DBCF72231172CEA86491489C52E1E202040E58F5C4887432E8C' or
    hash_ssn = E'\\x99138A59BEA78EFDF84329D7F6121ECA1B748084C0D6C7DD12B0673E5C3BDA41' or
    hash_ssn = E'\\xA3073D1DA1BB3012AA249FF2B13D881154407AEF2624AD772DFB5395F4942C94' or
    hash_ssn = E'\\xC6086DCF7BC0A96D33168B99D2CC74F9643E587158FDDEB7C7FF933DBFC6F040'
order by
    hash_ssn;


/* HASH_SSN4_DOB */

/* explore frequency distribution of SSNs */
select distinct
    hash_ssn4_dob,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    hash_ssn4_dob
order by
    count desc;
    

/* MNI_NO */

/* explore frequency distribution of
 * mni_nos */
select distinct
    mni_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
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
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
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
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    dob
order by
    count desc;

/* Look at all the records for one of the most
 * common dobs */
select *
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
where
    dob = timestamp '1992-06-03 00:00:00'
order by
    city;

/* Look at all the records for one of the most
 * common dobs */
select *
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
where
    dob = timestamp '1993-12-22 00:00:00'
order by
    city;


/* RACE */

/* list unique race values with counts */
select distinct
    race,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    race;

    
/* SEX */

/* list unique sex values with counts */
select distinct
    gender,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    gender;

    
/* CITY */
    
/* What cities are in data and how often?
 * Note that some cities are repeated in
 * sentence case and in upper case. */
select distinct
    city,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
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
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    state
order by
    count desc;
    

/* ZIP */

/* List unique zip codes and counts.
 * Note that some zip codes inlcude
 * last four but not all. Also, 3
 * Canadian zip codes in data */
select distinct
    zip,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    zip
order by
    zip desc;
    
    
/* BLOCK2010ID */
  
/* Examine frequencies block2010ids. */
select distinct
    blockgroup2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    blockgroup2010id
order by
    blockgroup2010id desc;


/* BLOCKGROUP2010ID */
  
/* Examine frequencies blockgroup2010ids. */
select distinct
    block2010id,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
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
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    tract2010id
order by
    tract2010id desc;    

    
/* GEOM */

/* Examine frequency distribution for 
 * tract2010ids. */
select distinct
    geom,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimsperson
) as jimsperson
group by
    geom
order by
    count desc;    


/* ********************************************
 *                   TESTS                    *
 * *******************************************/
