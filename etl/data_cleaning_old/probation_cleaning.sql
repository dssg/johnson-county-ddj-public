select distinct
	prob_type
from input.jocojimsprobation
order by prob_type;

select distinct
	prob_ofcr
from input.jocojimsprobation
order by prob_ofcr;


create or replace view premodeling.jocojimsprobation_clean AS
  (select 
    mni_no as mni_no,
    prob_no as prob_no,
    prob_start_dt as prob_start_dt,
    prob_end_dt as prob_end_dt,
    prob_ofcr as prob_ofcr,
    prob_type as prob_type,
    prob_ofcr as prob_ocfr,
    case_no as case_no,
    (select personid
      from input.jocojimsperson
      where input.jocojimsperson.mni_no = input.jocojimsprobation.mni_no)
      AS personid
from input.jocojimsprobation);