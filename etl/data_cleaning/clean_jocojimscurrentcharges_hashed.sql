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
    clean.jocojimscurrentcharges_hashed_view
as (
    select
        mni_no::int as mni_no,
        trim(both ' ' from case_no) as case_no,
        substring(case_no, '[JDCVR]+') as case_type,
        upper(charge_code) as charge_code,
        upper(substring(charge_code, '\d+-*\w*\d+')) as section,
        upper(charge_desc) as charge_desc,
        case
            when severity_lvl = 'NC' then null
            when severity_lvl = ' ' then null
            else upper(severity_lvl)
        end as severity_lvl,
        case
            when severity_lvl = ' ' then null
            when severity_lvl = 'NC' then null
            else split_part(severity_lvl, 'D', 1)
        end as severity_only,
        case
            when severity_lvl similar to '(\d)%' then 'FELONY'
            when severity_lvl = 'OG' then 'FELONY'
            when severity_lvl similar to '(A|B|C|D|F)%' then 'MISDEMEANOR'
            when severity_lvl = 'U' then 'INFRACTION'
        end as crime_class,
        case
            when severity_lvl = ' ' then null
            when severity_lvl similar to '%(D)' then true
            when severity_lvl not similar to '%(D)' then false
        end as drug_offense,
        upper(finding) as finding,
        case
            when finding similar to '(D|J)%' then false
            when finding similar to '(GU|NO)%' then true
        end as trial_occurred,
        case
            when finding similar to '(DIS|N)%' then false
            when finding similar to '(GU|G |J|DIV)%' then true
        end as found_or_plead_guilty,
        case
            when finding similar to '(DIS)%' then 'DISMISSED'
            when finding similar to '%(DIV)%' then 'DIVERSION'
            when finding similar to '(G)%' then 'GUILTY'
            when finding similar to '(N)%' then 'NOT GUILTY'
            when finding = 'OTHER TERMINATION' then 'OTHER TERMINATION'
            when finding = 'RELEASE FROM JURISDICTION' then 'RELEASE FROM JURISDICTION'
            when finding = 'STAY ORDER' then 'STAY ORDER'
            when finding = 'EXPUNGEMENT' then 'EXPUNGEMENT'
        end as coarse_finding,
        charge_pos as charge_pos
    from (
        select distinct
            *
        from
            public.jocojimscurrentcharges
    ) as charges);

/* drop the existing table and replace with a table
 * created from the view. */
drop table if exists clean.jocojimscurrentcharges_hashed cascade;
create table
    clean.jocojimscurrentcharges_hashed
as
    select
        *
    from
        clean.jocojimscurrentcharges_hashed_view;
