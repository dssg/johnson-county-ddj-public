/* Create view to present cleaned data.
 * This view makes the following changes:
 *
 * - Converts all string columns to upper
 * - Converts all missing string values to NULL rather
 *   than empty strings or special null values
 * - Encodes dates
 *
 * */
create or replace view
    clean.jocomentalhealthdiagnoses_view
as (
select
patid as patid,
date(dx_date) as dx_date,
dx_code as dx_code,
upper(dx_description) as dx_description
from (
        select distinct
            *
        from
            public.jocomentalhealthdiagnoses) as mentalhealthdiagnoses);

 /* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocomentalhealthdiagnoses cascade;
create table
    clean.jocomentalhealthdiagnoses
as
    select
        *
    from
        clean.jocomentalhealthdiagnoses_view;
