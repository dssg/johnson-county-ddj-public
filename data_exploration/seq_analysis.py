from seqpattern import generalized_transition
from seqpattern import sequence_gen
import psycopg2 as pg
import pandas as pd
import numpy as np
import pandas.io.sql as psql
import json
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy import create_engine

print "Initiating..."
seq_gen = sequence_gen.SequenceGen()
print "Event Number: ", len(seq_gen.get_events(start_date='1900-01-01', end_date = '2016-08-01')['dedupe_id'].unique())
print ""

def get_frequent_pattern(freq,window_size):
    subset_JM_J, subset_JE_J = seq_gen.generate_sequence_data(id_column='dedupe_id',freq=freq,window_size=window_size)

    print "subset_JM_J"
    pattern_JM_J = generalized_transition.find_frequent_patterns(tuple(subset_JM_J),min_support=len(subset_JM_J)*0.005,n=50)
    print pd.Series(pattern_JM_J).sort_values(ascending=False)
    print ""

    #print "subset_EM_J"
    #pattern_EM_J = generalized_transition.find_frequent_patterns(tuple(subset_JE_J),min_support=len(subset_JE_J)*0.005,n=50)
    #print pd.Series(pattern_EM_J).sort_values(ascending=False)
    #print ""


if __name__ == "__main__":
    print "Window size = 24 months"
    get_frequent_pattern('1M',24)
