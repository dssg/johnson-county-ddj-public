/* to replace the whole view, drop it before
 * creating the new view. Run the script
 * removing the regular expression for '--'.
 * Otherwise, it will try to create a new view
 * or update an old view without dropping.
 */
--drop view if exists clean.jocojims2juvinmatedata_view;
drop view if exists clean.jocojims2juvinmatedata_view;


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
    clean.jocojims2juvinmatedata_view
as (
    select
        inmate_no as inmate_no,
        case
            when char_length(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(dob, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
        end as dob,
        age as age,
        upper(s_e_x) as original_s_e_x,
        case
            when s_e_x = 'M' then 'MALE'
            when s_e_x = 'F' then 'FEMALE'
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
        mar_stat as mar_stat,
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
            when char_length(substring(regexp_replace(arrest_date, '[^0-9]', '', 'g') from 1 for 8)) < 8 then null
            else to_date(substring(regexp_replace(arrest_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
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
        unused as unused,
        religion as religion,
        mni::int as mni_no,
        stored_impound as stored_impound,
        location as location,
        relationship as relationship,
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
        unused2 as unused2,
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
        expunge_stamp as exp_stamp,
        complexion as complexion,
        cfn as cfn,
        demographic_code as demographic_code,
        pros_wksht__ as pros_wksht__,
        hisp as ethnic,
        admit_no as admit_no,
        alert_no as alert_no,
        kjdr_no as kjdr_no,
        col_bo as col_bo,
        col_bp as col_bp,
        col_bq as col_bq,
        col_br as col_br,
        case
            when
                substring(regexp_replace(admit_time, '[^0-9]', '', 'g') from '.{0,6}$') = '333333'
            then
                null
            when
                char_length(substring(regexp_replace(admit_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 4
            then
                to_timestamp(substring(regexp_replace(admit_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MI')::TIME
            when
                char_length(substring(regexp_replace(admit_time, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
            then
                to_timestamp(substring(regexp_replace(admit_time, '[^0-9]+', '') from '.{0,6}$'), 'HH24MISS')::TIME
        end as admit_time,
        admit_authority as admit_authority,
        hisp as hispanic,
        det_authority as det_authority,
        det_reason as det_reason,
        jdc_adm as jdc_adm,
        status_flg as status_flg,
        cinc_flg as cinc_flg,
        mun_flg as mun_flg,
        explain_flg as explain_flg,
        case
            when char_length(substring(mnh_refer_dt from 1 for 8)) < 8 then null
            else to_date(substring(mnh_refer_dt from 1 for 8), 'YYYYMMDD')
        end as mnh_refer_dt,
        diet as diet,
        printed_chow as printed_chow,
        drai_kdai_score as drai_kdai_score,
        drai_override_type as drai_override_type,
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
            public.jocojims2juvinmatedata) as inmates);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists
    clean.jocojims2juvinmatedata;
create table
    clean.jocojims2juvinmatedata
as
    select
        *
    from
        clean.jocojims2juvinmatedata_view;
