create or replace view
    clean.jocomentalhealth_hashed_view
as (
select
encode(hash_ssn,'hex') as hash_ssn,
encode(hash_ssn4,'hex') as hash_ssn4,
encode(hash_lname, 'hex') as hash_lname,
encode(hash_fname, 'hex') as hash_fname,
patid as patid,
dob as dob,
case
    when upper(race) similar to '(AM)%' THEN 'AMERICAN INDIAN OR ALASKA NATIVE'
    when upper(race) similar to '(AS)%' then 'ASIAN'
    when upper(race) similar to '(B)%' then 'BLACK OR AFRICAN AMERICAN'
    when upper(race) similar to '(C)%' then 'WHITE'
    when upper(race) similar to '(H)%' then 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER'
    when upper(race) similar to '(N)%' then null
    when upper(race) similar to '(O)%' then 'OTHER RACE'
    when upper(race) similar to '(U)%' then null
end as race,
case
    when sex similar to ' ' then null
    else upper(sex)
end as sex,
hash_gcstreetaddress as hash_gcstreetaddress,
upper(city) as city,
case
    when state like '  ' then null
    else state
end as state,
case
    when zip_code = ' ' then null
    else zip_code
end as zip,
gcvalidity as gcvalidity,
gcsrctbl as gcsrctabl,
gcscore as gcscore,
upper(gccity) as gccity,
upper(gccounty) as gccounty,
gcstate as gcstate,
gczipcode as gczipcode,
hash_gclonglat as hash_gclonglat,
gctract2010id as gctract2010id,
gcblockgroup2010id as gcblockgroup2010id,
gcblock2010id as gcblock2010id,
admit_date as admit_date,
dschrg_date as dschrg_date,
program as program,
pri_dx_code as pri_dx_code,
trim(trailing ' ' from upper(pri_dx_value)) as pri_dx_value,
trim(trailing ' ' from upper(refferal_source)) as refferal_source
from (
        select distinct
            *
        from
            public.jocomentalhealth) as mentalhealth);


/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocomentalhealth_hashed cascade;
create table
    clean.jocomentalhealth_hashed
as
    select
        *
    from
        clean.jocomentalhealth_hashed_view;

/* Add a unique case_id as the unique identifier for each row */
alter table clean.jocomentalhealth_hashed add column case_id SERIAL primary key;
