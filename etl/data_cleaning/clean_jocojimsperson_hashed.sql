/* to replace the whole view, drop it before
 * creating the new view. Run the script
 * removing the regular expression for '--'.
 * Otherwise, it will try to create a new view
 * or update an old view without dropping.
 */
--drop view if exists premodeling.jocojimsperson_hashed_view;
drop view if exists clean.jocojimsperson_hashed_view;

/* Create view to present cleaned data.
 * This view makes the following changes:
 *
 * - Converts all string columns to upper
 * - Converts all missing string values to NULL rather
 *   than empty strings or special null values
 * - Breaks US zips into two fields, containing
 *   the first five and last four (when available)
 * - Creates a bool indicating whether person is a
 *   JoCo resident or not based on whether there is a
 *   tract2010id and it is in the jocotract2010_pl
 *   table (true) or not (false)
 * - Creates a country value that indicates whether
 *   the addess is in the UNITED STATES or OTHER
 *   country
 * - Formats race and sex data according to categories
 *   used in the MED-ACT data and preserves
 *   original data in columns 'original_race' and
 *   'original_gender'
 * - Drops the geom column, which is always null
 *
 * */
create or replace view
    clean.jocojimsperson_hashed_view
as
    (
    select
        encode(hash_ssn,'hex') as hash_ssn,
        encode(hash_ssn4_dob,'hex') as hash_ssn4_dob,
        mni_no as mni_no,
        dob as dob,
        upper(race) as original_race,
        case
            when race = 'A' then 'ASIAN'
            when race = 'B' then 'BLACK OR AFRICAN AMERICAN'
            when race = 'I' then 'AMERICAN INDIAN OR ALASKA NATIVE'
            when race = 'O' then 'OTHER RACE'
            when race = 'U' then null
            when race = 'W' then 'WHITE'
        end as race,
        upper(gender) as original_gender,
        case
            when gender = 'M' then 'MALE'
            when gender = 'F' then 'FEMALE'
        end as sex,
        upper(city) as city,
        case
            when state = '' then null
            else state
        end as state,
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
        end as us_zip_last_four,
        tract2010id as tract2010id,
        blockgroup2010id as blockgroup2010id,
        block2010id as block2010id,
        exists (
            select
                *
            from
                public.jocoblock2010_pl
            where
                geoid10 = block2010id
        ) as joco_resident
    from (
        select distinct
            *
        from public.jocojimsperson ) as person);
/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocojimsperson_hashed;
create table
    clean.jocojimsperson_hashed
as
    select distinct
    *
    from
        clean.jocojimsperson_hashed_view;
