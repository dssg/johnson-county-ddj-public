/* This query creates a canonical events
 * table to be used for generating labels
 * and sequential features.
 */

create table
    clean.canonical_events
as
    select
        *
    from
        premodeling.dedupeid_event_dates;
