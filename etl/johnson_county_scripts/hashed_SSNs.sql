CREATE TABLE input.hashed_ssn AS (
  SELECT
    a.personid,
    crypt(a.personid::text, gen_salt('bf',8)) AS hashed_personid
  FROM 
    ( SELECT DISTINCT(personid) FROM jocoems ) AS a
);

