select count(*)
	personid
from input.jocoksparole
where personid is not null;

select distinct
	race
from input.jocoksparole
order by race;

select distinct
	sex
from input.jocoksparole;

select distinct
	race,
	case
		when race similar to '(BLACK)%' then 'BLACK OR AFRICAN AMERICAN'
		when race similar to '(ASIAN)%' then 'ASIAN'
		when race similar to '(AMERICAN)%' then 'AMERICAN INDIAN OR ALASKA NATIVE'
		when race similar to '(WHITE)%' then 'WHITE'
		when race similar to '(UNKNOWN)%' then 'NOT KNOWN'
	end as ems_race_category 
from input.jocoksparole
order by race;

select distinct
	race,
	case
		when split_part(race, '(', 2) = 'Hispanic)' then true
		else false
	end as hispanic
from input.jocoksparole
order by race;

SELECT DISTINCT 
	servicetype
FROM input.jocoksparole
ORDER BY servicetype;


SELECT DISTINCT 
	servicestatus
FROM input.jocoksparole
ORDER BY servicestatus;


create or replace view premodeling.ksparole_clean AS
  (select 
    personid as personid,
    dob as dob,
    upper(race) as race,
    case
		when race similar to '(BLACK)%' then 'BLACK OR AFRICAN AMERICAN'
		when race similar to '(ASIAN)%' then 'ASIAN'
		when race similar to '(AMERICAN)%' then 'AMERICAN INDIAN OR ALASKA NATIVE'
		when race similar to '(WHITE)%' then 'WHITE'
		when race similar to '(UNKNOWN)%' then 'NOT KNOWN'
	end as ems_race_category,
	case
		when split_part(race, '(', 2) = 'Hispanic)' then true
		else false
	end as hispanic,
	upper(sex) as sex,
	upper(city) as city,
	upper(state) as state,
	zip as zip,
	tract2010id as tract2010id,
	blockgroup2010id as blockgroup2010id,
	block2010id as block2010id,
	upper(servicetype) as servicetype,
	upper(servicestatus) as servicestatus,
    exists (select *
      from input.jocoblock2010_pl
      where geoid10 = block2010id)
      AS joco_resident
 from input.jocoksparole);
 