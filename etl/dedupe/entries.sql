drop table if exists clean.entries;
create table clean.entries as (
  select 'clean.jocoems_hashed' as src,
         event_id as foreign_id,
         'ems_' || event_id::text as entry_id,
         NULL as known_linked_entry_id,
         hash_ssn,
         hash_ssn4,
         hash_fname,
         hash_lname,
         left(dob::text,10) as dob,
         race,
         sex,
         gcresblock2010id as block2010id,
         incidentdate as date
  from clean.jocoems_hashed
union all
  -- JIMS uses a mutable master name table, but we need dates. Join against common tables for minimum dates where we can
  -- TODO: only 1/6th of JIMS name records get a date attached with this method! Are there other tables to include?
select 'jocojims2nameindexdata' as src,
         n.mni_no as foreign_id,
         'jims' || n.mni_no::text as entry_id,
         'jims' || coalesce(n.mni_ind,n.mni_no)::text as known_linked_entry_id,
         n.hash_ssn,
         n.hash_ssn4,
         n.hash_fname,
         n.hash_lname,
         left(n.dob::text,10) as dob,
         n.race,
         n.sex,
         n.gcblock2010id as block2010id,
         b.date as date
  from clean.jocojims2nameindexdata n
  	left join (select mni_no, least(min(bk_dt), min(arrest_dt)) as date
  		from clean.jocojims2inmatedata group by mni_no
  		union
  		select mni_no, least(min(bk_dt), min(arrest_dt)) as date
  		from clean.jocojims2juvinmatedata group by mni_no) as b using(mni_no)
union all
  -- same with MH — join against services for a minimum date
select 'mentalhealth_hashed' as src,
         case_id as foreign_id,
         'mh__' || case_id::text as entry_id,
         'mh_pat_' || patid::text as known_linked_entry_id,
         hash_ssn,
         hash_ssn4,
         hash_fname,
         hash_lname,
         left(dob::text,10) as dob,
         race,
         sex,
         gcblock2010id as block2010id,
         svc.date
  from clean.jocomentalhealth_hashed
    left join (select patid, min(svc_date) as date from clean.jocomentalhealthservices group by patid) as svc using (patid)
union all
  select 'rsitriage_hashed' as src,
         case_id as foreign_id,
         'rsi_' || case_id::text as entry_id,
         NULL as known_linked_entry_id,
         hash_ssn,
         hash_ssn4,
         hash_fname,
         hash_lname,
         left(dob::text,10) as dob,
         null as race,
         null as sex,
         null as blockgorup2010id,
         admitdate as date
  from clean.jocorsitriage_hashed);
  