-- TODO: This table is temporally leaky. Using it is fraught. Drop it entirely?
DROP TABLE IF EXISTS clean.individuals;
CREATE TABLE clean.individuals AS (
    SELECT
        dedupe_id,
        mode() WITHIN GROUP (ORDER BY hash_ssn) AS hash_ssn,
        mode() WITHIN GROUP (ORDER BY hash_lname) AS hash_lname,
        mode() WITHIN GROUP (ORDER BY hash_fname) AS hash_fname,
        mode() WITHIN GROUP (ORDER BY sex) AS sex,
        mode() WITHIN GROUP (ORDER BY dob) AS dob,
        mode() WITHIN GROUP (ORDER BY race) AS race
    FROM clean.entries GROUP BY dedupe_id);
