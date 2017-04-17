# ETL Pipeline

This directory contains the files used during ETL. It is coordinated by a
Drakefile, which is expected to be run from the repository root.

Empty touch-files are used to keep track of database state.

------------------------

## Miscellanea

A few other things are contained in this directory that are not currently used by the pipeline.

### Johnson County Scripts

In the directory `johnson_county_scripts`, 
* `hashed_SSNs.sql`: creates table of SSNs and their salted/hashed versions. Johnson County uses this script to anonymize the unique identifier before sending the data to DSSG and to de-anonymize the data when DSSG sends it back
* `update_hashed_SSNs.sh`: looks for new SSNs and adds them to `input.hashed_ssn`. 
