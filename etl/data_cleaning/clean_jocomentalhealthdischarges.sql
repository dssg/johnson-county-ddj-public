drop view if exists clean.jocomentalhealthdischarges_view;

create or replace view
    clean.jocomentalhealthdischarges_view
as (
select
patid as patid,
program as program,
date(admit_date) as admit_date,
date(dschg_date) as dschg_date,
upper(discharge_reason) as discharge_reason
from (
	select distinct * from public.jocomentalhealthdischarges) as discharge);

/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocomentalhealthdischarges;
create table
    clean.jocomentalhealthdischarges
as
    select distinct * from clean.jocomentalhealthdischarges_view;
