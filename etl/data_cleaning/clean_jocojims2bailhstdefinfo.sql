/* to replace the whole view, drop it before
 * creating the new view. Run the script
 * removing the regular expression for '--'.
 * Otherwise, it will try to create a new view
 * or update an old view without dropping.
 */
--drop view if exists clean.jocojims2bailmstdefinfo_view;
drop view if exists clean.jocojims2bailhstdefinfo_view;


/* Create view to present cleaned data.
 * This view makes the following changes:
 *
 * - Converts all string columns to upper
 * - Converts all missing string values to NULL rather
 *   than empty strings or special null values
 * - Creates a country value that indicates whether
 *   the addess is in the UNITED STATES or OTHER
 *   country
 * - Formats race and sex data according to categories
 *   used in the MED-ACT data
 * - Encodes dates, times, and hashes
 *
 * */
create or replace view
    clean.jocojims2bailhstdefinfo_view
as (
    select
        bail_no as bail_no,
        booking_ as booking_,
        case
            when char_length(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as dob,
        upper(rc) as original_race,
        case
            when rc = 'A' then 'ASIAN'
            when rc = 'B' then 'BLACK OR AFRICAN AMERICAN'
            when rc = 'I' then 'AMERICAN INDIAN OR ALASKA NATIVE'
            when rc = 'O' then 'OTHER RACE'
            when rc = 'U' then null
            when rc = 'W' then 'WHITE'
        end as race,
        upper(sex) as original_sex,
        case
            when sex = 'M' then 'MALE'
            when sex = 'F' then 'FEMALE'
        end as sex,
        city as city,
        state as state,
        zip_code as zip_code,
        prt as prt,
        amt_tendered as amt_tendered,
        mni_id::int as mni_no,
        gcvalidity as gcvalidity,
        gcsrctbl as gcsrctbl,
        gcscore as gcscore,
        gccity as gccity,
        gccounty as gccounty,
        gcstate as gcstate,
        gczipcode as gczipcode,
        gctract2010id as gctract2010id,
        gcblockgroup2010id as gcblockgroup2010id,
        gcblock2010id as gcblock2010id,
        encode(hash_ssn, 'hex') as hash_ssn,
        encode(hash_ssn4, 'hex') as hash_ssn4,
        encode(hash_lname, 'hex') as hash_lname,
        encode(hash_fname, 'hex') as hash_fname,
        geom as geom
    from (
        select distinct
            *
        from
            public.jocojims2bailhstdefinfo) as nameindex);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists
    clean.jocojims2bailhstdefinfo;
create table
    clean.jocojims2bailhstdefinfo
as
    select
        *
    from
        clean.jocojims2bailhstdefinfo_view;
