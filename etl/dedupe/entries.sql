drop table if exists dedupe.entries;
create table dedupe.entries as (
  select 'clean.jocoems_hashed' as src,
         event_id as foreign_id,
         'ems_' || event_id::text as entry_id,
         hash_ssn,
         hash_ssn4,
         hash_fname,
         hash_lname,
         left(dob::text,10) as dob,
         race,
         sex,
         gcresblock2010id as block2010id
  from clean.jocoems_hashed
union all
  select 'jocojims2nameindexdata' as src,
  		 mni_no::bigint as foreign_id,
         'jims' || mni_no::text as entry_id,
  		 hash_ssn,
         hash_ssn4,
  		 hash_fname,
  		 hash_lname,
         left(dob::text,10) as dob,
  		 race,
  		 sex,
  		 gcblock2010id as block2010id
  from clean.jocojims2nameindexdata
union all
  select 'mentalhealth_hashed' as src,
         case_id as foreign_id,
         'mh__' || case_id::text as entry_id,
         hash_ssn,
         hash_ssn4,
         hash_fname,
         hash_lname,
         left(dob::text,10) as dob,
         race,
         sex,
         gcblock2010id as block2010id
  from clean.jocomentalhealth_hashed
union all
  select 'rsitriage_hashed' as src,
         case_id as foreign_id,
         'rsi_' || case_id::text as entry_id,
         hash_ssn,
         hash_ssn4,
         hash_fname,
         hash_lname,
         left(dob::text,10) as dob,
         null as race,
         null as sex,
         null as blockgorup2010id
  from clean.jocorsitriage_hashed);
  