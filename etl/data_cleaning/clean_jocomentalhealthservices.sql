create or replace view
    clean.jocomentalhealthservices_view
as (
select
patid as patid,
date(svc_date) as svc_date,
therapist_num as therapist_num,
svc_code as svc_code,
upper(service_description) as service_description,
encode(hash_lname_therapist,'hex') as hash_lname_therapist,
encode(hash_fname_therapist,'hex') as hash_fname_therapist
from (
    select distinct * from public.jocomentalhealthservices) as mhs);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocomentalhealthservices cascade;
create table
    clean.jocomentalhealthservices
as
    select * from clean.jocomentalhealthservices_view;
