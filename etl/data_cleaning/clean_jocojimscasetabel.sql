drop view if exists clean.jocojimscasetable_view;
create or replace view
clean.jocojimscasetable_view
as (
select cd.case_no, cd.mni_id as mni_no,
substring(cd.case_no from '[A-Z][A-Z]') as case_type,
case
    when char_length(substring(regexp_replace(chp1.court_date, '[^0-9]', '', 'g') from 1 for 8)) < 8
    then null
    else to_date(substring(regexp_replace(chp1.court_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
end as first_court_date
from public.jocojims2casedata as cd
left join public.jocojims2casehearingpart1 as chp1 on cd.case_no = chp1.case_no
);

drop table if exists
    clean.jocojimscasetable;
create table
    clean.jocojimscasetable
as
    select
        *
    from
        clean.jocojimscasetable_view;
