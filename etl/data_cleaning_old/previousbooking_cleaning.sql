create or replace view premodeling.previousbooking_clean AS
  (select 
    mni_no as mni_no,
    booking_no as booking_no,
    case_no as case_no,
    prev_booking_no as prev_booking_no,
    prev_case_no as prev_case_no,
    (select personid
      from input.jocojimsperson
      where input.jocojimsperson.mni_no = input.jocojimspreviousbooking.mni_no)
      AS personid
from input.jocojimspreviousbooking);