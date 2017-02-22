/* to replace the whole view, drop it before
 * creating the new view. Run the script
 * removing the regular expression for '--'.
 * Otherwise, it will try to create a new view
 * or update an old view without dropping.
 */
drop view if exists clean.jocojims2nameindexdata_view;


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
create view
    clean.jocojims2nameindexdata_view
as (
    select
        mni_no::int as mni_no,
        case
            when alias_flag = 'Y' then TRUE
            else FALSE
        end::boolean as alias_flag,
        upper(gender) as original_gender,
        case
            when gender = 'M' then 'MALE'
            when gender = 'F' then 'FEMALE'
        end as sex,
        case
            when char_length(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as dob,
        eye as eye,
        skin as skin,
        license_state as license_state,
        license_year as license_year,
        jacket_no as jacket_no,
        pob_city as pob_city,
        pob_state as pob_state,
        res_city as res_city,
        res_state as res_state,
        res_zip as res_zip,
        height as height,
        weight as weight,
        upper(race) as original_race,
        case
            when race = 'A' then 'ASIAN'
            when race = 'B' then 'BLACK OR AFRICAN AMERICAN'
            when race = 'I' then 'AMERICAN INDIAN OR ALASKA NATIVE'
            when race = 'O' then 'OTHER RACE'
            when race = 'U' then null
            when race = 'W' then 'WHITE'
        end as race,
        hair as hair,
        state_id as state_id,
        ncic_cd as ncic_cd,
        vet_flag as vet_flag,
        case
            when char_length(substring(regexp_replace(photo_date, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(photo_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as photo_date,
        case
            when char_length(substring(regexp_replace(lab_date, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(lab_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as lab_date,
        case
            when char_length(substring(regexp_replace(print_date, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(print_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as print_date,
        employer as employer,
        emp_address as emp_address,
        emp_city as emp_city,
        emp_state as empstate,
        emp_zip as emp_zip,
        emp_phone as emp_phone,
        fi_affidavit as fi_affidavit,
        case
            when char_length(substring(regexp_replace(deceased_date, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(deceased_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as deceased_date,
        cc_flag as cc_flag,
        purge_flag as purge_flag,
        mni_ind::int as mni_ind,
        cms_cfn as cms_cfn,
        custody as custody,
        intelligence as intelligence,
        intelligence_agcy as intelligence_agcy,
        pring_agcy as pring_agcy,
        print_lab_no as print_lab_no,
        ethnicity as ethnicity,
        local as local,
        citizen as citizen,
        ncic as ncic,
        interpreter as interpreter,
        mni_type as mni_type,
        juvcms_cfn as juvcms_cfn,
        most_wanted_flag as most_wanted_flag,
        case
            when char_length(substring(regexp_replace(dna_dt, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(dna_dt, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as dna_dt,
        old_vet_flag as old_vet_flag,
        case
            when char_length(substring(regexp_replace(mnh_dt, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(mnh_dt, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as mnh_dt,
        alert_no as alert_no,
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
            public.jocojims2nameindexdata) as nameindex);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists
    clean.jocojims2nameindexdata;
create table
    clean.jocojims2nameindexdata
as
    select
        *
    from
        clean.jocojims2nameindexdata_view;
