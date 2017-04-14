create or replace view
    clean.jocorsitriage_hashed_view
as (
select
encode(hash_ssn,'hex') as hash_ssn,
encode(hash_ssn4, 'hex') as hash_ssn4,
encode(hash_lname, 'hex') as hash_lname,
encode(hash_fname, 'hex') as hash_fname,
date(dob) as dob,
date(admitdate) as admitdate,
case
    when upper(updatedcountyofresidence) = 'UNKNOWN' then null
    else upper(updatedcountyofresidence)
end as updatedcountyofresidence,
case
    when upper(countyother) = 'NONE' then null
    when upper(countyother) = 'UNKNOWN' then null
    when upper(countyother) !~ '^[A-Z]' then null
    else upper(countyother)
end as countyother,
case
    when timein similar to '%a'
    then LPAD(regexp_replace(substring(regexp_replace(timein, 'a', ':00','g') from 1 for 8),'[^0-9]','','g')::text,6,'0')::TIME
    when timein similar to '%p'
    then regexp_replace((regexp_replace(substring(regexp_replace(timein, 'p', ':00','g') from 1 for 8),'[^0-9]','','g')::int + 120000)::text,'^24','00')::TIME
    when char_length(substring(regexp_replace(timein, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
    then to_timestamp(substring(regexp_replace(timein, '[^0-9]+', '') from '.{0,6}$'), 'HH24MISS')::TIME
    else null
end as timein,
upper(updatedtransportedtorsiby) as updatedtransportedtorsiby,
upper(updatediflawenforcement) as updatediflawenforcement,
upper(broughtinbyother) as broughtinbyother,
upper(ifhadnotcometorainbow) as ifhadnotcometorainbow,
upper(ifclientdidnotcometorsijail) as ifclientdidnotcometorsijail,
upper(ifclientdidnotcometorsicommunitymentalhealthcenter) as ifclientdidnotcometorsicommunitymentalhealthcenter,
upper(ifclientdidnotcometorsiother) as ifclientdidnotcometorsiother,
upper(disposition) as disposition,
case
    when timeofdisposition similar to '%a'
    then LPAD(regexp_replace(substring(regexp_replace(timeofdisposition, 'a', ':00','g') from 1 for 8),'[^0-9]','','g')::text,6,'0')::TIME
    when timeofdisposition similar to '%p'
    then regexp_replace((regexp_replace(substring(regexp_replace(timeofdisposition, 'p', ':00','g') from 1 for 8),'[^0-9]','','g')::int + 120000)::text,'^24','00')::TIME
    when char_length(substring(regexp_replace(timeofdisposition, '[^0-9]', '', 'g') from '.{0,6}$')) = 6
    then to_timestamp(substring(regexp_replace(timeofdisposition, '[^0-9]+', '') from '.{0,6}$'), 'HH24MISS')::TIME
    else null
end as timeofdisposition,
upper(other) as other,
upper(otherhospital) as otherhospital,
upper(ifdispositionhospital) as ifdispositionhospital,
case
    when upper(primaryinsurance) = 'N/A' then null
    else upper(primaryinsurance)
end as primaryinsurance,
case
    when upper(secondaryinsurance) = 'N/A' then null
    else upper(secondaryinsurance)
end as secondaryinsurance,
upper(sasassessment) as sasassessment,
case
    when upper(clientofwyandotmh) similar to 'NEVER|NEVER BEEN' then 'NEVER'
    else upper(clientofwyandotmh)
end as clientofwyandotmh,
case
    when upper(clientofjocomh) similar to 'NEVER|NEVER BEEN' then 'NEVER'
    else upper(clientofjocomh)
end as clientofjocomh,
clienthasotoorder
from (
    select distinct *
    from public.jocorsitriage) as rsi);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocorsitriage_hashed cascade;
create table
    clean.jocorsitriage_hashed
as
    select * from clean.jocorsitriage_hashed_view;

/* Add a unique case_id as the unique identifier for each row */
alter table clean.jocorsitriage_hashed add column case_id SERIAL primary key;
