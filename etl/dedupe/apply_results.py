# -*- coding: utf-8 -*-
# Simple python script to add the computed dedupe_id to the clean.* tables

import sys
import json

import psycopg2 as psy
import psycopg2.extras

with open(sys.argv[1]) as f:
    config = json.load(f)
con = psy.connect(cursor_factory=psycopg2.extras.RealDictCursor, **config)

c = con.cursor()

# TODO: DO THIS BETTER -- loop over tables, specify schema, use pure SQL?
print('clean.jocoems_hashed')
c.execute("ALTER TABLE clean.jocoems_hashed DROP COLUMN IF EXISTS dedupe_id CASCADE")
c.execute("ALTER TABLE clean.jocoems_hashed ADD COLUMN dedupe_id INTEGER")
c.execute("UPDATE clean.jocoems_hashed dst SET dedupe_id = src.dedupe_id "
              "FROM clean.entries src WHERE src.foreign_id = dst.event_id and src.src='clean.jocoems_hashed'")

for t in ['jocojims2nameindexdata', 'jocojims2inmatedata', 'jocojims2juvinmatedata',
          'jocojims2bailmstdefinfo', 'jocojimsbailtable', 'jocojims2probation', 'jocojimscasetable']:
    print('clean.%s' % t)
    c.execute("ALTER TABLE clean.{table} DROP COLUMN IF EXISTS dedupe_id CASCADE".format(table=t))
    c.execute("ALTER TABLE clean.{table} ADD COLUMN dedupe_id INTEGER".format(table=t))
    c.execute("UPDATE clean.{table} dst SET dedupe_id = src.dedupe_id "
              "FROM clean.entries src WHERE src.foreign_id = dst.mni_no and src.src='jocojims2nameindexdata'".format(table=t))

print('clean.jocorsitriage_hashed')
c.execute("ALTER TABLE clean.jocorsitriage_hashed DROP COLUMN IF EXISTS dedupe_id CASCADE")
c.execute("ALTER TABLE clean.jocorsitriage_hashed ADD COLUMN dedupe_id INTEGER")
c.execute("UPDATE clean.jocorsitriage_hashed dst SET dedupe_id = src.dedupe_id "
              "FROM clean.entries src WHERE src.foreign_id = dst.case_id and src.src='rsitriage_hashed'")

print('clean.jocomentalhealth_hashed')
c.execute("ALTER TABLE clean.jocomentalhealth_hashed DROP COLUMN IF EXISTS dedupe_id CASCADE".format(table=t))
c.execute("ALTER TABLE clean.jocomentalhealth_hashed ADD COLUMN dedupe_id INTEGER".format(table=t))
c.execute("UPDATE clean.jocomentalhealth_hashed dst SET dedupe_id = src.dedupe_id "
          "FROM clean.entries src WHERE src.foreign_id = dst.case_id and src.src='mentalhealth_hashed'".format(table=t))
for t in ['jocomentalhealthdiagnoses', 'jocomentalhealthdischarges', 'jocomentalhealthservices']:
    print('clean.%s' % t)
    c.execute("ALTER TABLE clean.{table} DROP COLUMN IF EXISTS dedupe_id CASCADE".format(table=t))
    c.execute("ALTER TABLE clean.{table} ADD COLUMN dedupe_id INTEGER".format(table=t))
    c.execute("UPDATE clean.{table} dst SET dedupe_id = src.dedupe_id "
              "FROM clean.jocomentalhealth_hashed src WHERE src.patid = dst.patid".format(table=t))

con.commit()
c.close()
con.close()
