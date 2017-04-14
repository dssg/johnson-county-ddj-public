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
    clean.jocojims2inmatedata_view
as (
    select distinct
        inmate_no as inmate_no,
        case
            when char_length(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as dob,
        age as age,
        upper(sex) as original_sex,
        case
            when sex = 'M' then 'MALE'
            when sex = 'F' then 'FEMALE'
        end as sex,
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
        eyes as eyes,
        height as height,
        weight as weight,
        upper(mar_stat) as mar_stat,
        upper(city) as city,
        case
            when state = '' then null
            else state
        end as state,
        case
            when zip_code = ' ' then null
            else zip_code
        end as zip,
        case
            when zip_code ~ '^(\d{5})' then 'UNITED STATES'
            when zip_code is null then null
            when zip_code = ' ' then null
            else 'OTHER'
        end as country,
        birth_place as birth_place,
        arr_agcy as arr_agency,
        upper(case_number) as original_case_number,
        case
            when regexp_replace(upper(case_number), '[^A-Z0-9]+', '') = '' then null
            when regexp_replace(upper(case_number), '[^A-Z0-9]+', '') is null then null
            else regexp_replace(upper(case_number), '[^A-Z0-9]+', '')
        end as case_no,
        case
            when char_length(substring(arrest_dt from 1 for 8)) < 8 then null
            else to_date(substring(arrest_dt from 1 for 8), 'YYYYMMDD')
        end as arrest_dt,
        case
            when
                substring(regexp_replace(arrest_time, '[^0-9]', '', 'g') from '.{0,6}$') = '333333'
            then
                null
            when
                char_length(substring(regexp_replace(arrest_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 4
            then
                to_timestamp(substring(regexp_replace(arrest_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MI')::TIME
            when
                char_length(substring(regexp_replace(arrest_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
            then
                to_timestamp(substring(regexp_replace(arrest_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MISS')::TIME
        end as arrest_time,
        case
            when regexp_replace(trans_agency, '^[0-9]+', '') = '' then null
            else trans_agency
        end as trans_agency,
        case
            when regexp_replace(control_, '^[A-Z ]+', '') = '' then null
            else control_
        end as control_,
        case
            when char_length(substring(bk_dt from 1 for 8)) < 8 then null
            else to_date(substring(bk_dt from 1 for 8), 'YYYYMMDD')
        end as bk_dt,
        case
            when
                substring(regexp_replace(book_time, '[^0-9]', '', 'g') from '.{0,6}$') = '333333'
            then
                null
            when
                char_length(substring(regexp_replace(book_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 4
            then
                to_timestamp(substring(regexp_replace(book_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MI')::TIME
            when
                char_length(substring(regexp_replace(book_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
            then
                to_timestamp(substring(regexp_replace(book_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MISS')::TIME
        end as book_time,
        case
            when bonding = 'Y' then 'Y'
            else null
        end as bonding,
        arr_location as arr_location,
        b_state as b_state,
        mni::int as mni_no,
        location as location,
        license_state as license_state,
        veh_lic__ as veh_lic__,
        veh_lic_state as veh_lic_state,
        veh_lic_year as veh_lic_year,
        veh_make as veh_make,
        veh_model as veh_model,
        veh_color as veh_color,
        dna_draw as dna_draw,
        l_e_case_ as l_e_case_,
        same_as_ as same_as_,
        case
            when char_length(substring(rel_date from 1 for 8)) < 8 then null
            else to_date(substring(rel_date from 1 for 8), 'YYYYMMDD')
        end as rel_date,
        case
            when
                substring(regexp_replace(release_time, '[^0-9]', '', 'g') from '.{0,6}$') = '333333'
            then
                null
            when
                char_length(substring(regexp_replace(release_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 4
            then
                to_timestamp(substring(regexp_replace(release_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MI')::TIME
            when
                char_length(substring(regexp_replace(release_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
            then
                to_timestamp(substring(regexp_replace(release_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MISS')::TIME
        end as release_time,
        associated_records as associated_records,
        driver_s_lic_type as driver_s_lic_type,
        new_booked_inmate___orientation as new_booked_inmate___orientation,
        exp_stamp as exp_stamp,
        complexion as complexion,
        cfn as cfn,
        demographic_code as demographic_code,
        pros_wksht__ as pros_wksht__,
        ethnic as ethnic,
        case
            when char_length(substring(mnh_refer_dt from 1 for 8)) < 8 then null
            else to_date(substring(mnh_refer_dt from 1 for 8), 'YYYYMMDD')
        end as mnh_refer_dt,
        lgbt as lgbt,
        at_risk as at_risk,
        exploited as exploited,
        past_year as past_year,
        concerns as concerns,
        int_dt as int_dt,
        notify_asap as notify_asap,
        notify_verbal as notify_verbal,
        religion as religion,
        diet as diet,
        rstr_liv_cond as rstr_liv_cond,
        printed_chow as printed_chow,
        prev_bkno as prev_bkno,
        ofndr_reg_req as ofndr_reg_req,
        ofndr_stamp as ofndr_stamp,
        ofndr_match_type as ofndr_match_type,
        ofndr_key as ofndr_key,
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
            public.jocojims2inmatedata
        where mni ~ '\s*\d+\s*') as inmates);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocojims2inmatedata cascade;
create table
    clean.jocojims2inmatedata
as
    select
        *
    from
        clean.jocojims2inmatedata_view;
