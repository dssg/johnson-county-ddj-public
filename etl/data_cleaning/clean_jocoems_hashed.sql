/* to replace the whole view, drop it before
 * creating the new view. Run the script
 * removing the regular expression '--'.
 * Otherwise, it will try to create a new view
 * or update an old view without dropping.
 */
drop view if exists clean.jocoems_hashed_view;


/* Create view to present cleaned data.
 * This view makes the following changes:
 *
 * - Converts all string columns to upper
 * - Trims leading and trailing spaces from strings
 * - Converts all missing string values to NULL rather
 *   than empty strings or special null values
 * - Breaks US zips into two fields, containing
 *   the first five and last four (when available)
 * - Creates a bool indicating whether person is a
 *   JoCo resident or not based on whether there is a
 *   tract2010id and it is in the jocotract2010_pl
 *   table (true) or not (false)
 * - Drops time from the dob field
 * - Creates rescounty column that indicates whether
 *   the addess is in the UNITED STATES or OTHER
 *   country
 * - Creates a variable that contains only the date
 *   of an EMS call timestamp (discards time)
 * - Created two new columns indicating whether the
 *   disposition contains "transported" (true/false)
 *   or "treated" (true/false)
 * - Adds mni_nos based on hashed SSNs
 * - Adds a column indicating the number of other
 *   rows with the same timestamp for timestamps
 *   with exact times
 * - Converts the homeless variable to a bool
 * - Drops the geom column, which is always NULL
 * - Adds a unique event_id column
 *
 * */
create or replace view
    clean.jocoems_hashed_view
as
    (select distinct
        encode(hash_ssn,'hex') as hash_ssn,
        encode(hash_ssn4,'hex') as hash_ssn4,
        encode(hash_lname,'hex') as hash_lname,
        encode(hash_fname,'hex') as hash_fname,
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
            when upper(sex) = 'UNKNOWN' then null
            else upper(sex)
        end as sex,
        case
            when homeless = 1 then true
            when homeless is null then false
        end as homeless,
        exists (
            select
                *
            from
                public.jocoems
            where
                hash_ssn = ems.hash_ssn and
                homeless = 1
        ) as ever_homeless,
        encode(hash_gcresstreetaddress,'hex') as hash_gcresstreetaddress,
        case
            when rescity similar to '%(DESOTO|DE SOTO)%' then 'DESOTO'
            when rescity similar to '%(SHAWNEE|SHAWNEE MISSION)%' then 'SHAWNEE'
            else upper(rescity)
        end as rescity,
        case
            when upper(resstate) = '' then null
            else upper(resstate)
        end as resstate,
        case
            when reszip = ' ' then null
            else reszip
        end as reszip,
        case
            when reszip ~ '^(\d{5})' then 'UNITED STATES'
            when reszip is null then null
            when reszip = ' ' then null
            else 'OTHER'
        end as rescountry,
        case
            when reszip ~ '^(\d{5})' then split_part(reszip, '-', 1)
        end as us_reszip_first_five,
        case
            when reszip ~ '^(\d{5})' and char_length(reszip) > 5 then split_part(reszip, '-', 2)
        end as us_reszip_last_four,
        gcresvalidity as gcresvalidity,
        gcressrctbl as gcressrctbl,
        gcresscore as gcresscore,
        gcrescity as gcrescity,
        gcrescounty as gcrescounty,
        gcresstate as gcresstate,
        gcreszip as gcreszip,
        encode(hash_gcreslonglat,'hex') as hash_gcreslonglat,
        gcrestract2010id as gcrestract2010id,
        gcresblockgroup2010id as gcresblockgroup2010id,
        gcresblock2010id as gcresblock2010id,
        encode(hash_gccallstreetaddress,'hex') as hash_gccallstreetaddress,
        exists (select *
            from public.jocoblock2010_pl
            where geoid10 = gcresblock2010id)
        as joco_resident,
        upper(callcity) as callcity,
        case
            when upper(callstate) = '' then null
            else upper(callstate)
        end as callstate,
        case
            when callzip = ' ' then null
            else callzip
        end as callzip,
        case
            when callzip ~ '^(\d{5})' then 'UNITED STATES'
            when callzip is null then null
            when callzip = ' ' then null
            else 'OTHER'
        end as callcountry,
        case
            when callzip ~ '^(\d{5})' then split_part(callzip, '-', 1)
        end as us_callzip_first_five,
        case
            when callzip ~ '^(\d{5})' and char_length(callzip) > 5 then split_part(callzip, '-', 2)
        end as us_callzip_last_four,
        gccallvalidity as gccallvalidity,
        gccallsrctbl as gccallsrctbl,
        gccallscore as gccallscore,
        gccallcity as gccallcity,
        gccallcounty as gccallcounty,
        gccallstate as gccallstate,
        gccallzip as gccallzip,
        encode(hash_gccalllonglat,'hex') as hash_gccalllonglat,
        gccalltract2010id as gccalltract2010id,
        gccallblockgroup2010id as gccallblockgroup2010id,
        gccallblock2010id as gccallblock2010id,
        incidentdate,
        date(incidentdate) as incidentdate_dateonly,
        upper(primaryimpression) as primaryimpression,
        case
            when triage = '' then null
            else upper(trim(both ' ' from triage))
        end as triage,
        case
            when upper(disposition) is null then null
            when upper(disposition) = 'N/A' then null
            when upper(disposition) = 'NOT APPLICABLE' then null
            else upper(disposition)
        end as disposition,
        case
            when upper(disposition) similar to '%(TRANSPORTED)%' then true
            when disposition is null then null
            else false
        end as transported,
        case
            when upper(disposition) similar to '%(TREATED)%' then true
            when disposition is null then null
            else false
        end as treated,
        case
            when upper(destination) = 'OVERLAND PARK REGIONAL MEDICAL CENT' then 'OVERLAND PARK REGIONAL MEDICAL CENTER'
            else upper(destination)
        end as destination,
        encode(hash_serviceprovidername,'hex') as hash_serviceprovidername
     from (
         select distinct
             *
         from
             public.jocoems
     ) as ems);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocoems_hashed;
create table
    clean.jocoems_hashed
as
    select * from clean.jocoems_hashed_view;

/* Add a unique event_id as the unique identifier for each row */
alter table clean.jocoems_hashed add column event_id SERIAL primary key;
