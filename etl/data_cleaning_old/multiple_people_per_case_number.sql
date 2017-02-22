select 
	prev_case_no,
	count(distinct mni_no)
from
	input.jocojimspreviousbooking
group by
	prev_case_no
order by
	count desc;
	
	
select 
	case_no,
	count(distinct mni_no)
from
	input.jocojimspreviousbooking
group by
	case_no
order by
	count desc;
	
	
select
	case_no,
	count(distinct mni_no)
from
	input.jocojimsjailbooking
group by
	case_no
order by
	count desc;
	
	
select
 *
from
	input.jocojimsjailbooking
where
	case_no = '14DV01586' or
	case_no = '15JV00602' or
	case_no = '10CR01715' or
	case_no = '14JV00715' or
	case_no = '14DV01367';
	
select
 *
from
	input.jocojimscurrentcharges
where
	case_no = '14DV01586' or
	case_no = '15JV00602' or
	case_no = '10CR01715' or
	case_no = '14JV00715' or
	case_no = '14DV01367'
order by
	case_no,
	charge_desc;
	
select
 *
from
	input.jocojimspretrialservice
where
	case_no = '14DV01586' or
	case_no = '15JV00602' or
	case_no = '10CR01715' or
	case_no = '14JV00715' or
	case_no = '14DV01367'
order by
	case_no;