/* Create view to present cleaned data.
 * This view makes the following changes:
 *
 * - Converts all string columns to upper
 * - Converts all missing string values to NULL rather
 *   than empty strings or special null values
 * - Encodes dates, times, and hashes
 *
 * */
create or replace view
    clean.jocojims2probation_view
as (
select
    prob_no as prob_no,
    court_case as court_case,
    mni::int as mni_no,
    city as city,
    case
        when st = ' ' then null
        else st
    end as state,
    case
        when zip = ' ' then null
        else zip
    end as zip,
    case
        when char_length(substring(prob_due_dt from 1 for 8)) < 8 then null
        else to_date(substring(prob_due_dt from 1 for 8), 'YYYYMMDD')
    end as prob_due_dt,
    case
        when char_length(substring(actual_pb_compl_dt from 1 for 8)) < 8 then null
        else to_date(substring(actual_pb_compl_dt from 1 for 8), 'YYYYMMDD')
    end as actual_pb_compl_dt,
    psp_hours as psp_hours,
    psp_comp as psp_comp,
    case
        when (char_length(substring(psp_compl_dt from 1 for 8)) < 8) or (char_length(regexp_replace(psp_compl_dt,'/.*/','')) < 8) then null
        else to_date(substring(psp_compl_dt from 1 for 8), 'YYYYMMDD')
    end as psp_compl_dt,
    rest__amt_ as rest_amt,
    rest__compl_ as rest_compl,
    case
        when char_length(substring(rest__dt_ from 1 for 8)) < 8 then null
        else to_date(substring(rest__dt_ from 1 for 8), 'YYYYMMDD')
    end as rest_dt,
    seminar_amount as seminar_amount,
    seminar_compl as seminar_compl,
    case
        when (char_length(substring(seminar_dt from 1 for 8)) < 8) or (char_length(regexp_replace(seminar_dt,'[^0-9]','')) > 8) then null
        else to_date(substring(seminar_dt from 1 for 8), 'YYYYMMDD')
    end as seminar_dt,
    type_counselling as type_counselling,
    counsel_compl as counsel_compl,
    case
        when char_length(substring(counsel_compl_dt from 1 for 8)) < 8 then null
        else to_date(substring(counsel_compl_dt from 1 for 8), 'YYYYMMDD')
    end as counsel_compl_dt,
    other_prob as other_prob,
    fine_hrs as fine_hrs,
    total_hrs as total_hrs,
    work_hours as work_hours,
    case
        when char_length(substring(no_later_than from 1 for 8)) < 8  then null
        else to_date(substring(no_later_than from 1 for 8), 'YYYYMMDD')
    end as no_later_than,
    contact_prob as contact_prob,
    start_work as start_work,
    case
        when (char_length(substring(psp_act_compl from 1 for 8)) < 8) or (char_length(regexp_replace(psp_act_compl,'/.*/','')) < 8)then null
        else to_date(substring(psp_act_compl from 1 for 8), 'YYYYMMDD')
    end as psp_act_compl_dt,
    hrs_compl as hrs_compl,
    case
        when (char_length(substring(proof_revd from 1 for 8)) < 8) or (char_length(regexp_replace(proof_revd,'/.*/','')) < 8) then null
        else to_date(substring(proof_revd from 1 for 8), 'YYYYMMDD')
    end as proof_revd,
    proof as proof,
    case
        when psp_status = 'OPEN' then 'OPEN'
        when psp_status = 'CLOSED' then 'CLOSED'
        when psp_status = 'O' then 'OPEN'
        when psp_status = 'C' then 'CLOSED'
        else null
    end as psp_status,
    psp as psp,
    place_of_birth as place_of_birth,
    lenght_of_employment as length_of_employment,
    type_of_work as type_of_work,
    week_hrly_wage as week_hrly_wage,
    case
        when (char_length(substring(dna_draw_dt from 1 for 8)) < 8) or (char_length(regexp_replace(dna_draw_dt,'/.*/','')) < 8) then null
        else to_date(substring(dna_draw_dt from 1 for 8), 'YYYYMMDD')
    end as dna_draw_dt,
    col_bf as col_bf,
    marital_status as marital_status,
    total_no_child as total_no_child,
    living_w_defnd as living_w_defnd,
    case
        when (char_length(substring(discharge_date from 1 for 8)) < 8) or (char_length(regexp_replace(discharge_date,'/.*/','')) < 8) then null
        else to_date(substring(discharge_date from 1 for 8), 'YYYYMMDD')
    end as discharge_date,
    discharge_type as discharge_type,
    comm_serv_agcy_name as comm_serv_agcy_name,
    rn as rn,
    case
        when (char_length(substring(reassess_date from 1 for 8)) < 8) or (char_length(regexp_replace(reassess_date,'/.*/','')) < 8) then null
        else to_date(substring(reassess_date from 1 for 8), 'YYYYMMDD')
    end as reassess_date,
    baseline_test as baseline_test,
    comm_serv as comm_serv,
    educa_tion as education,
    coun_seling as counseling,
    case
        when (char_length(substring(begin_date from 1 for 8)) < 8) or (char_length(regexp_replace(begin_date,'/.*/','')) < 8) then null
        else to_date(substring(begin_date from 1 for 8), 'YYYYMMDD')
    end as begin_date,
    case
        when (char_length(substring(end_date from 1 for 8)) < 8) or (char_length(regexp_replace(end_date,'/.*/','')) < 8) then null
        else to_date(substring(end_date from 1 for 8), 'YYYYMMDD')
    end as end_date,
    drug_test_freq as drug_test_freq,
    case
        when (char_length(substring(baseline_date from 1 for 8)) < 8) or (char_length(regexp_replace(baseline_date,'/.*/','')) < 8) then null
        else to_date(substring(baseline_date from 1 for 8), 'YYYYMMDD')
    end as baseline_date,
    baseline_result as baseline_result,
    dept as dept,
    override_level as override_level,
    case
        when char_length(substring(lsir_or_dt from 1 for 8)) < 8 then null
        else to_date(substring(lsir_or_dt from 1 for 8), 'YYYYMMDD')
    end as lsir_or_dt,
    consecutive as consecutive,
    case
        when char_length(substring(dna_print from 1 for 8)) < 8 then null
        else to_date(substring(dna_print from 1 for 8), 'YYYYMMDD')
    end as dna_print,
    box__ as box,
    acc__ as acc,
    box__date as box__date,
    case
        when char_length(substring(lsir_enter_dt from 1 for 8)) < 8 then null
        else to_date(substring(lsir_enter_dt from 1 for 8), 'YYYYMMDD')
    end as lsir_enter_dt,
    ethn as ethn,
    doc__ as doc,
    case
        when char_length(substring(ice_date from 1 for 8)) < 8 then null
        else to_date(substring(ice_date from 1 for 8), 'YYYYMMDD')
    end as ice_date,
    gpa_at_start as gpa_at_start,
    gpa_at_end as gpa_at_end,
    pro_social as pro_social,
    pe_score_1 as pe_score_1,
    pe_score_2 as pe_score_2,
    pe_score_3 as pe_score_3,
    pe_score_4 as pe_score_4,
    parent_pre_test_score as parent_pre_test_score,
    parent_post_test as parent_post_test_score,
    client_pre_test_score as client_pre_test_score,
    client_post_test_score as client_post_test_score,
    case
        when char_length(substring(off_link_date from 1 for 8)) < 8 then null
        else to_date(substring(off_link_date from 1 for 8), 'YYYYMMDD')
    end as off_link_date,
    mail_in as mail_in,
    dv_sir as dv_sir,
    panel as panel,
    active_drug_flg,
    case
        when char_length(substring(avert_req_dt from 1 for 8)) < 8 then null
        else to_date(substring(avert_req_dt from 1 for 8), 'YYYYMMDD')
    end as avert_req_dt,
    gcvalidity as gcvalidity,
    gcsrctbl as gcsrctbl,
    gcscore as gcscore,
    gccity as gccity,
    gccounty as gccounty,
    gcstate as gcstate,
    gczipcode as gczipcode,
    gctract2010id as gctract2010id,
    gcblockgroup2010id as gcblockgroup2010id,
    gcblock2010id as gcblock2010id,
    geom as geom
    from (
        select distinct
            *
        from
            public.jocojims2probation) as probation);
/* drop the existing table and replace with a table
 * created from the view.
 */
drop table if exists clean.jocojims2probation cascade;
create table
    clean.jocojims2probation
as
    select
        *
    from
        clean.jocojims2probation_view;
