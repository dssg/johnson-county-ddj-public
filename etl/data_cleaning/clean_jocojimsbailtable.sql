drop view if exists clean.jocojimsbailtable_view;
create or replace view
    clean.jocojimsbailtable_view
as (
select
    hdef.bail_no,
    hdef.amt_tendered,
    hdet.bail_pay,
    hdet."type" as bail_type,
    case
        when hdef.amt_tendered is null then false
        else true
    end as bail_out,
    case
        when
            char_length(substring(regexp_replace(hdet.receipt_date, '[^0-9]', '', 'g') from 1 for 8)) < 8
        then
            null
        else
            to_date(substring(regexp_replace(hdet.receipt_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
    end as receipt_date,
    hdef.mni_id::int as mni_no,
    hdef.booking_ as booking_no
from
    jocojims2bailhstdefinfo as hdef
left join
    jocojims2bailhstdetail as hdet
on
    hdet.bail_no = hdef.bail_no
union all
select
    mdef.bail_no,
    mdef.amt_tendered,
    mdet.bail_pay,
    mdet."type" as bail_type,
    case
        when mdef.amt_tendered is null then false
        else true
    end as bail_out,
    case
        when
            char_length(substring(regexp_replace(mdet.receipt_date, '[^0-9]', '', 'g') from 1 for 8)) < 8
        then
            null
        else
            to_date(substring(regexp_replace(mdet.receipt_date, '[^0-9]', '', 'g') from 1 for 8), 'YYYYMMDD')
    end as receipt_date,
    mdef.mni_id::int as mni_no,
    mdef.booking_ as booking_no
from
    jocojims2bailmstdefinfo as mdef
left join
    jocojims2bailmstdetail as mdet
on
    mdet.bail_no = mdef.bail_no
);

drop table if exists
    clean.jocojimsbailtable;
create table
    clean.jocojimsbailtable
as
    select
        *
    from
        clean.jocojimsbailtable_view;
