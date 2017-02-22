from tqdm import tqdm
import pandas as pd
import numpy as np
from itertools import islice, product

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

def generate_subsequence_pool(sequence_data,n=2):
    ''' Return a pool of all the sub-sequences from sequence data list table, which contains all the sub-sequences extracted from the sequences.

    :params list seqence_data: An input sequence data list table.
    :params int n: sub-sequence size
    :return: A list of sub-sequence pool.
    :rtype: list
    '''
    s_pool = []
    for i in range(len(sequence_data)):
        s_pool.extend(generate_subsequence(sequence_data[i],n))
    return s_pool

def extract_feature(seq,n=2,show_zero_feature=True):
    ''' Return the direct transition feature which is counts of all types of sub-sequence in the sequence.

    :params list seq: An input sequence to be extracted feature from
    :params int n: sub-sequence size
    :params bool show_zero_feature: If True, shows all the features. If False, shows only non-zero features.
    :return: A dictionary of sequential feature, which keys are sub-sequence type and values are the counts in the sequence.
    :rtype: dictionary
    '''
    patterns = generate_subsequence(seq,n)

    if show_zero_feature:
        all_types_of_transition = []
        for p in product('EJM',repeat=n):
            all_types_of_transition.append("".join(map(lambda x: x + '->',p))[:-2])

        feature = {key:0 for key in all_types_of_transition}

        for p in patterns:
            feature[p] += 1
        return feature
    else:
        feature = {key:0 for key in np.unique(np.array(patterns))}
        for p in patterns:
            feature[p] += 1
        return feature

def generate_subseq_dist(sequence_data,n=2):
    '''Return a distribution of sub-sequence from a sequence data list table

    :params list sequence_data: An input sequence data list table
    :params int n: sub-sequence size
    :return: A pandas Series of the distribution of all the possible sub-sequences
    :rtype: pandas.Series
    '''
    all_types_of_transition = []
    for p in product('EJM',repeat=n):
        all_types_of_transition.append("".join(map(lambda x: x + '->',p))[:-2])
    feature_dist = {key:0 for key in all_types_of_transition}

    feature_counts = pd.Series(generate_subsequence_pool(sequence_data,n)).value_counts()
    for i, d in zip(feature_counts.index, feature_counts):
        feature_dist[i] = d
    return pd.Series(feature_dist)
