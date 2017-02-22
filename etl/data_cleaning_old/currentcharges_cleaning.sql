
/* find unique values for each column */
select distinct
 upper(charge_code) as charge_code, 
 charge_desc
from input.jocojimscurrentcharges
order by charge_code;

select distinct
 severity_lvl
from input.jocojimscurrentcharges
order by severity_lvl;

select distinct
  charge_desc,
  severity_lvl
from input.jocojimscurrentcharges
where severity_lvl = 'AD';

select distinct
  charge_desc,
  severity_lvl
from input.jocojimscurrentcharges
where severity_lvl = 'OG';

select distinct
  charge_desc,
  severity_lvl
from input.jocojimscurrentcharges
where severity_lvl = 'NC';

select distinct
  charge_desc,
  severity_lvl
from input.jocojimscurrentcharges
where severity_lvl = 'U';

select distinct
  finding
from input.jocojimscurrentcharges
order by finding;

select distinct
  finding,
  charge_desc
from input.jocojimscurrentcharges
where finding = 'DIVERSION TRAFFIC';

select count(*)
  finding
from input.jocojimscurrentcharges
where finding = 'OTHER TERMINATION';

select count(*)
  finding
from input.jocojimscurrentcharges
where finding = 'STAY ORDER';

select distinct
  severity_lvl
from input.jocojimscurrentcharges
where severity_lvl similar to '(\d)%'
order by severity_lvl;

select distinct
  severity_lvl
from input.jocojimscurrentcharges
where severity_lvl similar to '(A|B|C|D|F)%'
order by severity_lvl;

select distinct
  severity_lvl,
  case
    WHEN severity_lvl = ' ' THEN null
      WHEN severity_lvl similar to '%(D)' THEN true
      WHEN severity_lvl not similar to '%(D)' THEN false
      
    END as drug_offense
from input.jocojimscurrentcharges
order by severity_lvl;

select distinct
  severity_lvl,
  CASE
      WHEN severity_lvl similar to '(\d)%' THEN 'felony'
      WHEN severity_lvl = 'OG' THEN 'felony'
      WHEN severity_lvl similar to '(A|B|C|D|F)%' THEN 'misdemeanor'
  END as felony_or_misdemeanor
from input.jocojimscurrentcharges
order by severity_lvl;

select distinct
  finding,
  case
      when finding similar to '(D|J|G )%' THEN false
      when finding similar to '(GU|NO)%' then true
    end as trial_occurred
from input.jocojimscurrentcharges
order by finding;

select distinct
  finding,
  case
      when finding similar to '(DIS|N)%' THEN false
      when finding similar to '(GU|G |J|DIV)%' then true
    end as found_or_plead_guilty
from input.jocojimscurrentcharges
order by finding;

select distinct
  finding,
  case
      when finding similar to '(DIS)%' THEN 'DISMISSED'
      when finding similar to '%(DIV)%' then 'DIVERSION'
      when finding similar to '(G)%' then 'GUILTY'
      when finding similar to '(N)%' then 'NOT GUILTY'
      when finding = 'OTHER TERMINATION' then 'OTHER TERMINATION'
      when finding = 'RELEASE FROM JURISDICTION' then 'RELEASE FROM JURISDICTION'
      when finding = 'STAY ORDER' then 'STAY ORDER'
      when finding = 'EXPUNGEMENT' then 'EXPUNGEMENT'
    end as coarse_finding
from input.jocojimscurrentcharges
order by finding;

select mni_no from input.jocojimsperson;
      
      
/* create view to present cleaned data */
create or replace view premodeling.currentcharges_clean AS
  (select 
    mni_no as mni_no,
    case_no as case_no,
    upper(charge_code) as charge_code,
    upper(charge_desc) as charge_desc,
    upper(severity_lvl) as severity_lvl,
    CASE
      WHEN severity_lvl similar to '(\d)%' THEN 'felony'
      WHEN severity_lvl = 'OG' THEN 'felony'
      WHEN severity_lvl similar to '(A|B|C|D|F)%' THEN 'misdemeanor'
    END as felony_or_misdemeanor,
    case
      WHEN severity_lvl = ' ' THEN null
      WHEN severity_lvl similar to '%(D)' THEN true
      WHEN severity_lvl not similar to '%(D)' THEN false
    END as drug_offense,
    upper(finding) as finding,
    case
      when finding similar to '(DIS|J|G |O)%' THEN false
      when finding similar to '(GU|NO)%' then true
    end as trial_occurred,
    case
      when finding similar to '(DIS|N)%' THEN false
      when finding similar to '(GU|G |J|DIV|O)%' then true
    end as found_or_plead_guilty,
    case
      when finding similar to '(DIS)%' THEN 'DISMISSED'
      when finding similar to '%(DIV|OTHER)%' then 'DIVERSION'
      when finding similar to '(G)%' then 'GUILTY'
      when finding similar to '(N)%' then 'NOT GUILTY'
      when finding = 'RELEASE FROM JURISDICTION' then 'RELEASE FROM JURISDICTION'
      when finding = 'STAY ORDER' then 'STAY ORDER'
      when finding = 'EXPUNGEMENT' then 'EXPUNGEMENT'
    end as coarse_finding,
    charge_pos as charge_pos,
    (select personid
      from input.jocojimsperson
      where input.jocojimsperson.mni_no = input.jocojimscurrentcharges.mni_no)
      AS personid
 from input.jocojimscurrentcharges);
