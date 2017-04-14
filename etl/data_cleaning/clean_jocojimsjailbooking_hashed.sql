/* to replace the whole view, drop it before
 * creating the new view. Run the script
 * removing the regular expression for '--'.
 * Otherwise, it will try to create a new view
 * or update an old view without dropping.
 */
--drop view if exists premodeling.jocojimsjailbooking_hashed_view;
drop view if exists clean.jocojimsjailbooking_hashed_view;

/* Create view to present cleaned data.
 * This view makes the following changes:
 *
 * - Converts all string columns to upper
 * - Converts all missing string values to NULL rather
 *   than empty strings or special null values
 * - Creates a new variable for the KS statute section
 *   to which a charge refers
 * - Removes erroneous 'NC' values from severity_lvl
 * - Adds crime_class column indicating whether an
 *   offense is a felony, misdemeanor, or infraction
 * - Adds a column indicating whether an offense is a
 *   drug offense
 * - Adds severity_only that gives severity level
 *   regardless of drug offense status
 * - Creates a column indicating whether a trial
 *   occurred
 * - Creates a column indicating whether the
 *   defendant was found or plead guilty
 * - Created a coarse_finding column that
 *   divides findings into fewer categories
 * - Drops the geom column, which is always null
 *
 * */
create or replace view
    clean.jocojimsjailbooking_hashed_view
as (
    select
        mni_no::int as mni_no,
        trim(both ' ' from case_no) as case_no,
        substring(case_no, '[JDCVR]+') as case_type,
        booking_no as booking_no,
        upper(arresting_agency) as arresting_agency,
        case
            when arresting_agency similar to '%(CAMPUS|SCHOOL)%' then 'SCHOOL'
            when arresting_agency similar to '%(SHERIFF)%' then 'COUNTY SHERIFF'
            when arresting_agency similar to '(KANSAS)%' then 'STATE'
            when arresting_agency similar to '(OTHER)%' then 'OTHER AGENCY'
            when arresting_agency similar to '%(P.D.)' then 'CITY'
            when arresting_agency similar to '%(PARK)%' then 'COUNTY PARK DISTRICT'
        end as arresting_agency_type,
        booking_date as booking_date,
        release_date as release_date,
        bail_type as bail_type,
        bail_amt as bail_amt,
        case
            when bail_amt is not null then true
            when bail_amt is null then false
        end as bailed_out
    from (
        select distinct *
            from public.jocojimsjailbooking) as booking);

/* drop the existing table and replace with a table
 * created from the view. */
drop table if exists
    clean.jocojimsjailbooking_hashed;
create table
    clean.jocojimsjailbooking_hashed
as
    select
        *
    from
        clean.jocojimsjailbooking_hashed_view;
