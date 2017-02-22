from tqdm import tqdm
import pandas as pd
import numpy as np
from itertools import islice, product
import datetime
import user_timeline
import sqlalchemy
import json
import re

CONFIG_PATH = '/mnt/data/johnson_county/config/default_profile.json'
with open(CONFIG_PATH) as f:
    config = json.load(f)
engine = sqlalchemy.create_engine('postgres://', connect_args=config)


class SequenceGen(object):
    def __init__(self,start_date='2010-01-01', end_date='2016-08-01'):
        self.connection = engine
        self.events = self.get_events(start_date,end_date)

    def get_events(self, start_date='2010-01-01', end_date = '2016-08-01'):
        events =  pd.read_sql("SELECT dedupe_id, event, begin_date, end_date FROM clean.canonical_events where begin_date >= '{}' and begin_date <= '{}'".format(start_date,end_date), self.connection)
        events['event'] = events['event'].map({'ems':'E','mh':'M','booking':'J'})
        return events

    def generate_sequence_data(self, id_column='dedupe_id', freq='1M',window_size=6):
        '''Return a sequence data list of all the people in the events table.
        :params DataFrame df: events table which has columns of personid, event and begin_date
        :params int window: time window of months
        :return: A list of temporally oredered path sequences for all the people in the events table
        :rtype: list
        '''
        df = self.events
        n = len(df)
        #personid_all = []
        #subset_E_J_noM = []
        #subset_EM_J =[]
        subset_M_J_noE = []
        #subset_E_M_noJ= []
        #subset_J_J = []
        subset_JM_J = []
        subset_JE_J = []

        personid = df[id_column].unique()
        for p in tqdm(personid[:n]):
            df_p = df[df[id_column] == p]
            user = get_series(df_p,freq)
            sequence = "".join(user.values)
            subsequence = generate_subsequence(user,window_size)
            #sequence = extract_transition_sequence(df_p)
            #personid_all.append(p)
            #subsequence_all.extend(subsequence)

            #if find_pattern('EJ', sequence) and 'M' not in sequence:
            #    subset_E_J_noM.extend(subsequence)

            if find_pattern('MJJ', sequence) or find_pattern('JMJ', sequence):
                subset_JM_J.extend(subsequence)

            if find_pattern('EJJ', sequence) or find_pattern('JEJ', sequence):
                subset_JE_J.extend(subsequence)

            if find_pattern('MJ', sequence) and 'E' not in sequence:
                subset_M_J_noE.extend(subsequence)

            #if find_pattern('EM', sequence) and 'J' not in sequence:
            #    subset_E_M_noJ.extend(subsequence)

            #if find_pattern('JJ', sequence):
            #    subset_J_J.extend(subsequence)

        return subset_JM_J, subset_JE_J, subset_M_J_noE#subset_E_J_noM, subset_EM_J, subset_M_J_noE, subset_E_M_noJ, subset_J_J


def get_series(user,freq='1M'):
    user_df = user.sort_values(by='begin_date')
    user_series = pd.Series(list(user_df['event']), index=user_df['begin_date'])
    user_series.index = pd.DatetimeIndex(user_series.index)
    user_resample = user_series.resample(freq).sum().astype('str').replace(r'0', "", regex=True)
    #time = pd.date_range(start='2010-01',end='2016-06',freq=freq)
    #user_resample = user_resample.reindex(time,fill_value="").astype('str')
    #for i in range(len(user_resample)):
    #        user_resample[i] = "".join(user_resample[i].split('0'))
    return user_resample


def sliding_window(seq, n=2):
    '''A generator that yield a 1-dimemsion sliding window generator that can iterate over a list. The next() method returns a sub-list which size is n.
    :params list seq: An input list to be iterated by the sliding window
    :params int n: sub-sequence size
    :return: A generator
    :rtype: generator object
    '''
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

def generate_subsequence(seq, n=2):
    ''' Return a list of sub-sequences of string type from a sequence which is symbolized as 'a->b'. The sub-sequence size is decided by n
    :params list seq: An input sequence to be generated sub-sequences from
    :params int n: sub-sequence size
    :return: A list of sub-sequences
    :rtype: list
    '''

    p_gen = sliding_window(seq, n)
    pattern_data = []
    try:
        while True:
            window = "".join(next(p_gen))
            if window:
                pattern_data.append(window)
            #pattern_data.append("".join(map(lambda p: p + '->',next(p_gen)))[:-2])
    except StopIteration:
        pass
    finally:
        del p_gen
    return pattern_data


def extract_transition_sequence(df):
    '''Returns a list of services according to the date and service, which is in order of time.
    :params DataFrame df: A DataFrame with columns date and service
    :return: A list of services in order of time
    :rtype: list
    '''
    # Drop the row with NaN in date column
    result = df.sort_values('begin_date').dropna(subset=['begin_date'])

    sequence = list(result['event'])
    return sequence

def sequence_viz(sequence):
    return "".join(map(lambda p: p + '->', sequence))[:-2]

def sequence2list(sequence):
    '''Convert a string type of a person's temporal path pattern to a list
    :params str pattern: A string type of a person's temporal path pattern generated from extract_path_pattern()
    :return: A list of temporal path pattern
    :rtype: list
    '''
    path_sequence = []
    path_sequence.append(sequence.split('->'))
    path_sequence = [p.strip() for p in path_sequence[0]]
    return path_sequence

def find_pattern(pattern, sequence):
    ''' Check if the user-specified pattern is in the sequence. True, if it exsists.
    :params str pattern: A string of user-specified pattern.
    :params str pattern: A string of target sequence to be checked.
    :return: True if the pattern exsists. False if not.
    :rtype: bool
    '''
    if isinstance(sequence,list):
        sequence = "".join(sequence)

    re_pattern = "".join(map(lambda p: p + '.*', pattern))[:-2]
    m = re.search(re_pattern,sequence)
    try :
        m.group(0)
        return True
    except AttributeError:
        return False



# def generate_sequence_data(df, id_column='hash_ssn', n=10):
#     '''Return a sequence data list of all the people in the events table.

#     :params DataFrame df: events table which has columns of personid, event and begin_date
#     :params int n: number of sequence data to be generated
#     :return: A list of temporally oredered path sequences for all the people in the events table
#     :rtype: list
#     '''
#     n = len(df)
#     personid_all = []
#     sequence_all = []

#     personid_reduced = []
#     sequence_reduced = []

#     personid_jail = []
#     sequence_jail = []

#     personid_ems = []
#     sequence_ems = []

#     personid_mh = []
#     sequence_mh = []

#     personid = df[id_column].unique()
#     for p in tqdm(personid[:n]):
#         df_p = df[df[id_column] == p]
#         user = user_timeline(p)
#         sequence = extract_transition_sequence(df_p)
#         personid_all.append(p)
#         sequence_all.append(sequence)
#         # Some condtions on generating the sequence data
#         #if len(sequence) > 10:
#         #    personid_reduced.append(p)
#         #    sequence_reduced.append(sequence)
#         #if 'J' in sequence:
#         #    personid_jail.append(p)
#         #    sequence_jail.append(sequence)
#         #if 'E' in sequence:
#         #    personid_ems.append(p)
#         #    sequence_ems.append(sequence)
#         #if 'M' in sequence:
#         #    personid_mh.append(p)
#         #    sequence_mh.append(sequence)

#     return {'personid':personid_all, 'sequence':sequence_all}#, {'personid':personid_jail, 'sequence':sequence_jail}, #\
#            #{'personid':personid_ems, 'sequence':sequence_ems}, {'personid': personid_mh, 'sequence': sequence_mh}
