#!/bin/bash

# Check for environment variables
: "${PGHOST:?Need to set PGHOST}"
: "${PGUSER:?Need to set PGUSER}"
: "${PGDATABASE:?Need to set PGDATABASE}"

psql <<SQL
INSERT INTO input.hashed_ssn
  SELECT
    a.personid,
    crypt(a.personid::text, gen_salt('bf',8)) as hashed_personid
  FROM
    ( SELECT DISTINCT(personid) FROM jocoems ) AS a
  LEFT JOIN
    ( SELECT personid FROM input.hashed_ssn ) AS b ON a.personid = b.personid
  WHERE
    b.personid IS NULL;
SQL
