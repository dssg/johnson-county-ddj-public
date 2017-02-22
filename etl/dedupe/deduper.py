# -*- coding: utf-8 -*-

"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.
With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the PostgresSQL database.
__Note:__ You will need to run `python pgsql_big_dedupe_example_init_db.py`
before running this script.
For smaller datasets (<10,000), see our
[csv_example](http://datamade.github.io/dedupe-examples/docs/csv_example.html)
"""
import os
import csv
import tempfile
import time
import logging
import optparse
import locale
import json

import psycopg2 as psy
import psycopg2.extras

import dedupe

optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
optp.add_option('-c', '--config', dest='CONFIG_PATH', action='store_const',
                default='/mnt/data/johnson_county/config/default_profile.json',
                help='Path to JSON config file that contains host, database, user and password')
(opts, args) = optp.parse_args()
log_level = logging.WARNING
if opts.verbose == 1:
    log_level = logging.INFO
elif opts.verbose is None or opts.verbose >= 2:
    log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

settings_file = 'dedup_postgres_settings'
training_file = 'dedup_postgres_training.json'

start_time = time.time()
with open(opts.CONFIG_PATH) as f:
    config = json.load(f)
con = psy.connect(cursor_factory=psycopg2.extras.RealDictCursor, **config)

c = con.cursor()

FIELD_NAMES = "entry_id, hash_ssn, hash_fname, hash_lname, dob, race, sex, block2010id"
INDIVIDUAL_SELECT = "SELECT %s FROM dedupe.entries" % FIELD_NAMES

# ## Training

if False: #os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=4)
else:

    # Define the fields dedupe will pay attention to
    #
    # The address, city, and zip fields are often missing, so we'll
    # tell dedupe that, and we'll learn a model that take that into
    # account
    fields = [{'field': 'hash_ssn', 'variable name': 'hash_ssn', 'type': 'Exact', 'has missing': True},
              {'field': 'hash_fname', 'variable name': 'hash_fname', 'type': 'Exact', 'has missing': True},
              {'field': 'hash_lname', 'variable name': 'hash_lname', 'type': 'Exact', 'has missing': True},
              {'field': 'dob', 'variable name': 'dob', 'type': 'String', 'has missing': True},
              {'field': 'race', 'type': 'Categorical', 'has missing': True,
                  'categories': ['ASIAN', 'OTHER RACE', 'WHITE',
                                 'AMERICAN INDIAN OR ALASKA NATIVE',
                                 'BLACK OR AFRICAN AMERICAN',
                                 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER']},
              {'field': 'sex', 'type': 'Categorical', 'has missing': True,
                  'categories': ['MALE', 'FEMALE']},
              {'field': 'block2010id', 'variable name': 'block2010id', 'type': 'Exact', 'has missing': True},
              {'type': 'Interaction',
                   'interaction variables': ['hash_lname', 'dob']},
              {'type': 'Interaction',
                   'interaction variables': ['hash_lname', 'hash_fname']},
              {'type': 'Interaction',
                   'interaction variables': ['hash_ssn', 'dob']},
              ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # Named cursor runs server side with psycopg2
    cur = con.cursor('individual_select')

    cur.execute(INDIVIDUAL_SELECT)
    temp_d = dict((i, row) for i, row in enumerate(cur))

    deduper.sample(temp_d, 7500)
    del temp_d
    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            deduper.readTraining(tf)

    # ## Active learning

    print('starting active labeling...')
    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    dedupe.convenience.consoleLabel(deduper)
    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # Notice our two arguments here
    #
    # `maximum_comparisons` limits the total number of comparisons that
    # a blocking rule can produce.
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(maximum_comparisons=500000000, recall=0.90)

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()

## Blocking
print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and record ids
print('creating blocking_map database')
c.execute("DROP TABLE IF EXISTS dedupe.blocking_map")
c.execute("CREATE TABLE dedupe.blocking_map "
          "(block_key VARCHAR(200), entry_id VARCHAR(20))")


# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.blocker.index_fields:
    c2 = con.cursor('c2')
    c2.execute("SELECT DISTINCT %s FROM dedupe.entries" % field)
    field_data = (row[field] for row in c2)
    deduper.blocker.index(field_data, field)
    c2.close()

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(block_key, donor_id)` tuples.
print('writing blocking map')

c3 = con.cursor('donor_select2')
c3.execute(INDIVIDUAL_SELECT)
full_data = ((row['entry_id'], row) for row in c3)
b_data = deduper.blocker(full_data)

# Write out blocking map to CSV so we can quickly load in with
# Postgres COPY
csv_file = tempfile.NamedTemporaryFile(prefix='blocks_', delete=False, mode='w')
csv_writer = csv.writer(csv_file)
csv_writer.writerows(b_data)
c3.close()
csv_file.close()

f = open(csv_file.name, 'r')
c.copy_expert("COPY dedupe.blocking_map FROM STDIN CSV", f)
f.close()

os.remove(csv_file.name)

con.commit()


# Remove blocks that contain only one record, sort by block key and
# donor, key and index blocking map.
#
# These steps, particularly the sorting will let us quickly create
# blocks of data for comparison
print('prepare blocking table. this will probably take a while ...')

logging.info("indexing block_key")
c.execute("CREATE INDEX blocking_map_key_idx ON dedupe.blocking_map (block_key)")

c.execute("DROP TABLE IF EXISTS dedupe.plural_key")
c.execute("DROP TABLE IF EXISTS dedupe.plural_block")
c.execute("DROP TABLE IF EXISTS dedupe.covered_blocks")
c.execute("DROP TABLE IF EXISTS dedupe.smaller_coverage")

# Many block_keys will only form blocks that contain a single
# record. Since there are no comparisons possible withing such a
# singleton block we can ignore them.
logging.info("calculating dedupe.plural_key")
c.execute("CREATE TABLE dedupe.plural_key "
          "(block_key VARCHAR(200), "
          " block_id SERIAL PRIMARY KEY)")

c.execute("INSERT INTO dedupe.plural_key (block_key) "
          "SELECT block_key FROM dedupe.blocking_map "
          "GROUP BY block_key HAVING COUNT(*) > 1")

logging.info("creating dedupe.block_key index")
c.execute("CREATE UNIQUE INDEX block_key_idx ON dedupe.plural_key (block_key)")

logging.info("calculating dedupe.plural_block")
c.execute("CREATE TABLE dedupe.plural_block "
          "AS (SELECT block_id, entry_id "
          " FROM dedupe.blocking_map INNER JOIN dedupe.plural_key "
          " USING (block_key))")

logging.info("adding entry_id index and sorting index")
c.execute("CREATE INDEX plural_block_id_idx ON dedupe.plural_block (entry_id)")
c.execute("CREATE UNIQUE INDEX plural_block_block_id_id_uniq "
          " ON dedupe.plural_block (block_id, entry_id)")


# To use Kolb, et.al's Redundant Free Comparison scheme, we need to
# keep track of all the block_ids that are associated with a
# particular donor records. We'll use PostgreSQL's string_agg function to
# do this. This function will truncate very long lists of associated
# ids, so we'll also increase the maximum string length to try to
# avoid this.
# c.execute("SET group_concat_max_len = 4096")

logging.info("creating dedupe.covered_blocks")
c.execute("CREATE TABLE dedupe.covered_blocks "
          "AS (SELECT entry_id, "
          " string_agg(CAST(block_id AS TEXT), ',' ORDER BY block_id) "
          "   AS sorted_ids "
          " FROM dedupe.plural_block "
          " GROUP BY entry_id)")

c.execute("CREATE UNIQUE INDEX covered_blocks_id_idx "
          "ON dedupe.covered_blocks (entry_id)")

con.commit()

# In particular, for every block of records, we need to keep
# track of a donor records's associated block_ids that are SMALLER than
# the current block's entry_id. Because we ordered the ids when we did the
# GROUP_CONCAT we can achieve this by using some string hacks.
logging.info("creating dedupe.smaller_coverage")
c.execute("CREATE TABLE dedupe.smaller_coverage "
          "AS (SELECT entry_id, block_id, "
          " TRIM(',' FROM split_part(sorted_ids, CAST(block_id AS TEXT), 1)) "
          "      AS smaller_ids "
          " FROM dedupe.plural_block INNER JOIN dedupe.covered_blocks "
          " USING (entry_id))")

con.commit()


## Clustering

def candidates_gen(result_set):
    lset = set

    block_id = None
    records = []
    i = 0
    for row in result_set:
        if row['block_id'] != block_id:
            if records:
                yield records

            block_id = row['block_id']
            records = []
            i += 1

            if i % 10000 == 0:
                print(i, "blocks")
                print(time.time() - start_time, "seconds")

        smaller_ids = row['smaller_ids']

        if smaller_ids:
            smaller_ids = lset(smaller_ids.split(','))
        else:
            smaller_ids = lset([])

        records.append((row['entry_id'], row, smaller_ids))

    if records:
        yield records

c4 = con.cursor('c4')
c4.execute("SELECT %s, block_id, smaller_ids FROM dedupe.smaller_coverage "
           "INNER JOIN dedupe.entries "
           "USING (entry_id) "
           "ORDER BY (block_id)" % FIELD_NAMES)

print('clustering...')
clustered_dupes = deduper.matchBlocks(candidates_gen(c4),
                                      threshold=0.5)

## Writing out results

# We now have a sequence of tuples of donor ids that dedupe believes
# all refer to the same entity. We write this out onto an entity map
# table
c.execute("DROP TABLE IF EXISTS dedupe.entity_map")

print('creating dedupe.entity_map database')
c.execute("CREATE TABLE dedupe.entity_map "
          "(entry_id VARCHAR(20), canon_id VARCHAR(20), "
          " cluster_score FLOAT, PRIMARY KEY(entry_id))")

csv_file = tempfile.NamedTemporaryFile(prefix='entity_map_', delete=False,
                                       mode='w')
csv_writer = csv.writer(csv_file)


for cluster, scores in clustered_dupes:
    cluster_id = cluster[0]
    for donor_id, score in zip(cluster, scores) :
        csv_writer.writerow([donor_id, cluster_id, score])

c4.close()
csv_file.close()

f = open(csv_file.name, 'r')
c.copy_expert("COPY dedupe.entity_map FROM STDIN CSV", f)
f.close()

os.remove(csv_file.name)

con.commit()

c.execute("CREATE INDEX head_index ON dedupe.entity_map (canon_id)")
con.commit()

# Print out the number of duplicates found
print('# duplicate sets')
print(len(clustered_dupes))


# ## Payoff

# Now the entity_map table only contains pairs that are matched across multiple
# entries; we want to create a lookup table that will contain all entries and
# link them to a cluster_id SERIAL.
c.execute("DROP TABLE IF EXISTS dedupe.map")
c.execute("CREATE TABLE dedupe.map "
          "AS SELECT COALESCE(canon_id, entry_id) AS canon_id,"
          "entry_id, "
          "left(entry_id, 4) as entry_src, "
          "substr(entry_id, 5)::int as entry_key, "
          "COALESCE(cluster_score, 1.0) AS cluster_score "
          "FROM dedupe.entity_map "
          "RIGHT JOIN dedupe.entries USING(entry_id)")

# Convert the canon_id to an integer id
c.execute("DROP TABLE IF EXISTS dedupe.clusters")
c.execute("CREATE TABLE dedupe.clusters AS "
          "(SELECT DISTINCT canon_id FROM dedupe.map)")
c.execute("ALTER TABLE dedupe.clusters ADD COLUMN cluster_id SERIAL UNIQUE")

# Add that cluster id back into the mapping table
c.execute("ALTER TABLE dedupe.map ADD COLUMN cluster_id INTEGER")
c.execute("UPDATE dedupe.map dst SET cluster_id = src.cluster_id "
          "FROM dedupe.clusters src WHERE dst.canon_id = src.canon_id")

# And speed up lookups by entry_src and entry_key
c.execute("CREATE INDEX ON dedupe.map (entry_key)")
c.execute("CREATE INDEX ON dedupe.map (entry_src)")

con.commit()

# Now we *also* have known links from the JIMS database between MNI numbers
# We want to merge clusters that are linked by MNI.
c.execute("""DROP TABLE IF EXISTS dedupe.links;
             CREATE TABLE dedupe.links AS (
                 SELECT
                     jims.mni_ind::int AS mni1,
                     jims.mni_no AS mni2
                 FROM clean.jocojims2nameindexdata JIMS
                 WHERE jims.mni_ind IS NOT NULL
             );
             ALTER TABLE dedupe.links ADD COLUMN cluster1 integer;
             ALTER TABLE dedupe.links ADD COLUMN cluster2 integer;
             UPDATE dedupe.links SET
                 cluster1 = map.cluster_id
                 FROM dedupe.map map
                 WHERE map.entry_src='jims' AND map.entry_key = mni1;
             UPDATE dedupe.links SET
                 cluster2 = map.cluster_id
                 FROM dedupe.map map
                 WHERE map.entry_src='jims' AND map.entry_key = mni2;""")
# As long as there are no multi-level links, we can just do this naively:
c.execute("""ALTER TABLE dedupe.map ADD COLUMN dedupe_id integer;
             UPDATE dedupe.map SET dedupe_id = cluster_id;
             UPDATE dedupe.map SET dedupe_id = src.cluster1
                 FROM dedupe.links src WHERE src.cluster2 = cluster_id;""")
con.commit()

# And now add that back to the existing tables
# TODO: DO THIS BETTER -- loop over tables, do it in a separate file?
c.execute("ALTER TABLE clean.jocoems_hashed DROP COLUMN IF EXISTS dedupe_id")
c.execute("ALTER TABLE clean.jocoems_hashed ADD COLUMN dedupe_id INTEGER")
c.execute("UPDATE clean.jocoems_hashed dst SET dedupe_id = src.dedupe_id "
              "FROM dedupe.map src WHERE src.entry_key = dst.event_id and src.entry_src='ems_'")

for t in ['jocojims2nameindexdata', 'jocojims2inmatedata', 'jocojims2juvinmatedata', 'jocojims2bailmstdefinfo', 'jocojims2probation']:
    c.execute("ALTER TABLE clean.{table} DROP COLUMN IF EXISTS dedupe_id".format(table=t))
    c.execute("ALTER TABLE clean.{table} ADD COLUMN dedupe_id INTEGER".format(table=t))
    c.execute("UPDATE clean.{table} dst SET dedupe_id = src.dedupe_id "
              "FROM dedupe.map src WHERE src.entry_key = dst.mni_no::int and src.entry_src='jims'".format(table=t))

c.execute("ALTER TABLE clean.jocorsitriage_hashed DROP COLUMN IF EXISTS dedupe_id")
c.execute("ALTER TABLE clean.jocorsitriage_hashed ADD COLUMN dedupe_id INTEGER")
c.execute("UPDATE clean.jocorsitriage_hashed dst SET dedupe_id = src.dedupe_id "
              "FROM dedupe.map src WHERE src.entry_key = dst.case_id and src.entry_src='rsi_'")

c.execute("ALTER TABLE clean.jocomentalhealth_hashed DROP COLUMN IF EXISTS dedupe_id".format(table=t))
c.execute("ALTER TABLE clean.jocomentalhealth_hashed ADD COLUMN dedupe_id INTEGER".format(table=t))
c.execute("UPDATE clean.jocomentalhealth_hashed dst SET dedupe_id = src.dedupe_id "
          "FROM dedupe.map src WHERE src.entry_key = dst.case_id and src.entry_src='mh__'".format(table=t))
for t in ['jocomentalhealthdiagnoses', 'jocomentalhealthdischarges', 'jocomentalhealthservices']:
    c.execute("ALTER TABLE clean.{table} DROP COLUMN IF EXISTS dedupe_id".format(table=t))
    c.execute("ALTER TABLE clean.{table} ADD COLUMN dedupe_id INTEGER".format(table=t))
    c.execute("UPDATE clean.{table} dst SET dedupe_id = src.dedupe_id "
              "FROM clean.jocomentalhealth_hashed src WHERE src.patid = dst.patid".format(table=t))

# And create an individuals table that simply uses the most commonly found values for each id
c.execute("""CREATE TABLE clean.individuals AS (
             WITH entries AS (
                 SELECT dedupe_id, hash_ssn, hash_fname, hash_lname, race, sex
                 FROM dedupe.entries a INNER JOIN dedupe.map b ON (a.entry_id=b.entry_id))
             SELECT
                 dedupe_id,
                 mode() WITHIN GROUP (ORDER BY hash_ssn) AS hash_ssn,
                 mode() WITHIN GROUP (ORDER BY hash_lname) AS hash_lname,
                 mode() WITHIN GROUP (ORDER BY hash_fname) AS hash_fname,
                 mode() WITHIN GROUP (ORDER BY race) AS race,
                 mode() WITHIN GROUP (ORDER BY sex) AS sex
             FROM entries GROUP BY dedupe_id);""")

con.commit()

# # Close our database connection
c.close()
con.close()

print('ran in', time.time() - start_time, 'seconds')
