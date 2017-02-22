/* This file conducts exploratory analyses in
 * preparation for cleaning the 
 * jocojimscurrentcharges table and runs tests
 * on the select statements used in the query
 * that creates the cleaned view. */

/* ********************************************
 *               EXPLORATIONS                 *
 * *******************************************/

/* MNI_NO */

/* Look at distribution of identifiers */
select distinct
    mni_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    mni_no
order by 
    count desc;

/* Look at identifies by charge position */
select distinct
    mni_no,
    charge_pos,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    mni_no,
    charge_pos
order by 
    count desc;


/* CASE_NO */

/* Look at frequencies of case numbers.
 * Never exceeds 3 */
select distinct
    case_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    case_no
order by 
    count desc;

/* How many 1s and 2s? */ 
select distinct
    case_no,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    case_no
order by 
    count;


/* CHARGE_CODE */

/* Look at dristributon of charge codes */
select distinct
    charge_code,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    charge_code
order by 
    count desc;

/* How is each charge code described? Note
 * that each charge code refers to a chapter,
 * article, and section of a statute, with 
 * subsections sometimes included. */
select distinct
    charge_code,
    charge_desc,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    charge_code,
    charge_desc
order by 
    charge_code;
    
    
/* CHARGE_DESC */

/* Look at dristributon of charge descriptions */
select distinct
    charge_desc,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    charge_desc
order by 
    count desc;
    
    
/* SEVERITY_LVL */

/* Look at dristributon of severity levels */
select distinct
    severity_lvl,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    severity_lvl
order by 
    count desc;

/* Look at some drug charges */    
select distinct
    charge_desc,
    severity_lvl
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    severity_lvl = 'AD';

/* What are the off grid offenses? */
select distinct
    charge_desc,
    severity_lvl
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    severity_lvl = 'OG';
    
/* NC stands for no contest. Robert claims
 * it is not a severity level, but it shows
 * up twice in the data. Likely error? */
select distinct
    charge_desc,
    severity_lvl,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    severity_lvl = 'NC'
group by
    charge_desc,
    severity_lvl;

/* What charges are unclassified? 
 * Appear to be mostly infractions. */
select distinct
    charge_desc,
    severity_lvl,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    severity_lvl = 'U'
group by
    charge_desc,
    severity_lvl;
    

/* FINDING */

/* Look at dristributon of findings */
select distinct
    finding,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    finding
order by 
    count desc;
    
/* What kinds of charges end in traffic
 * diversions? */
select distinct
    charge_desc,
    finding,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    finding = 'DIVERSION TRAFFIC'
group by
    charge_desc,
    finding
order by 
    count desc;
    
/* What kinds of charges end in other
 * termination (completion of diversion? */
select distinct
    charge_desc,
    finding,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    finding = 'OTHER TERMINATION'
group by
    charge_desc,
    finding
order by 
    count desc;
    
/* What kinds of charges end in stay orders? */
select distinct
    charge_desc,
    finding,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
where
    finding = 'STAY ORDER'
group by
    charge_desc,
    finding
order by 
    count desc;


/* CHARGE_POS */

/* Look at dristributon of charge positions */
select distinct
    charge_pos,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    charge_pos
order by 
    count desc;


/* GEOM */

/* Look at dristributon of geoms */
select distinct
    geom,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    geom
order by 
    count desc;



/* ********************************************
 *                 TESTING                    *
 * *******************************************/
   
/* Try converting charge code to section */
select distinct
    charge_code,
    charge_desc,
    upper(substring(charge_code, '\d+-*\w*\d+')) as section,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    charge_code,
    charge_desc,
    section
order by 
    charge_code; 

/* Extract case type (JV, CR, or DV)
 * from case number. */
select distinct
    case_no,
    substring(case_no, '[JDCVR]+') as case_type,
    count(*)
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
group by
    case_no,
    case_type; 

/* Test expression for creating crime_class
 * column */
select distinct
    severity_lvl,
    case
        when severity_lvl similar to '(\d)%' then 'FELONY'
        when severity_lvl = 'OG' then 'FELONY'
        when severity_lvl similar to '(A|B|C|D|F)%' then 'MISDEMEANOR'
        when severity_lvl = 'U' then 'INFRACTION'
    end as crime_class
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
order by
    severity_lvl;

/* Test code for creating drug_offense column */
select distinct
    severity_lvl,
    case
        when severity_lvl = ' ' then null
        when severity_lvl similar to '%(D)' then true
        when severity_lvl not similar to '%(D)' then false
    end as drug_offense
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
order by
    severity_lvl;
    
/* Test code for creating severity_only */
select distinct
    severity_lvl,
    case
        when severity_lvl = ' ' then null
        when severity_lvl = 'NC' then null
        else split_part(severity_lvl, 'D', 1)
    end as severity_only
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
order by
    severity_lvl;

/* Test expression for creating trial_occured */
select distinct
    finding,
    case
        when finding similar to '(D|J)%' then false
        when finding similar to '(GU|NO)%' then true
    end as trial_occurred
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
order by
    finding;

/* Test code for found_or_plead_guilty */
select distinct
    finding,
    case
        when finding similar to '(DIS|N)%' then false
        when finding similar to '(GU|G |J|DIV)%' then true
    end as found_or_plead_guilty
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
order by
    finding;

/* Test code for coarse_finding */
select distinct
    finding,
    case
        when finding similar to '(DIS)%' then 'DISMISSED'
        when finding similar to '%(DIV)%' then 'DIVERSION'
        when finding similar to '(G)%' then 'GUILTY'
        when finding similar to '(N)%' then 'NOT GUILTY'
        when finding = 'OTHER TERMINATION' then 'OTHER TERMINATION'
        when finding = 'RELEASE FROM JURISDICTION' then 'RELEASE FROM JURISDICTION'
        when finding = 'STAY ORDER' then 'STAY ORDER'
        when finding = 'EXPUNGEMENT' then 'EXPUNGEMENT'
    end as coarse_finding
from (
    select distinct
        *
    from
        public.jocojimscurrentcharges
) as charges
order by
    finding;
