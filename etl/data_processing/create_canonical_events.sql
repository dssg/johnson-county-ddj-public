/* This query creates a canonical events
 * table to be used for generating labels
 * and sequential features.
 */

drop view if exists clean.canonical_events_view;

create or replace view
    clean.canonical_events_view
as (
 SELECT mh.dedupe_id,
    'mh'::text AS event,
    mh.admit_date AS begin_date,
    mh.dschrg_date AS end_date
   FROM clean.jocomentalhealth_hashed mh
  WHERE (mh.dedupe_id IS NOT NULL)
UNION ALL
 SELECT ems.dedupe_id,
    'ems'::text AS event,
    ems.incidentdate AS begin_date,
    ems.incidentdate AS end_date
   FROM clean.jocoems_hashed ems
  WHERE (ems.dedupe_id IS NOT NULL)
UNION ALL
 SELECT booking.dedupe_id,
    'booking'::text AS event,
    booking.bk_dt AS begin_date,
    booking.rel_date AS end_date
   FROM clean.jocojims2inmatedata booking
UNION ALL
 SELECT booking.dedupe_id,
    'booking'::text AS event,
    booking.bk_dt AS begin_date,
    booking.rel_date AS end_date
   FROM clean.jocojims2juvinmatedata booking
  ORDER BY 1, 3
);

drop table if exists clean.canonical_events;
create table
    clean.canonical_events
as
    select * from clean.canonical_events_view;
