import pandas as pd
import numpy as np
import seqmining
from sequence_gen import sequence_viz
import re

def list2str(sequences_list):
    ''' Convert a list of sequence lists into a tuple of sequence strings.
    For example, [ ['E','M','J'], ['E','E','E']] -> ('EMJ',''EEE')

    :params list sequence_list: A list of sequence lists
    :return: A tuple of sequences in type of string
    :rtype: tuple
    '''
    sequences_str = []
    for s in sequences_list:
        sequences_str.append("".join(s))

    sequences_str = tuple(sequences_str)
    return sequences_str

def find_frequent_patterns(sequences_str, min_support_ratio=0.1, n=None):
    ''' Return a dictionary of frequent patterns which key is the pattern and value is related support.
    A frequent pattern is a set of items with a support greater than minimum support which is set to 100 in default.

    :params tuple sequences_str: A tuple of sequences in type of string.
    :params int min_support: Minimum support.
    :params int n: Most n frequent patterns.
    :return: A dictionary of frequent patterns.
    :rtype: dictionary

    '''
    min_support = len(sequences_str) * min_support_ratio

    freq_seqs = seqmining.freq_seq_enum(sequences_str, min_support)
    pattern = []
    support = []
    for seq in list(freq_seqs):
        pattern.append("".join(map(lambda p: p + '->', list(seq)[0]))[:-2])
        support.append(list(seq)[1])

    if n:
        support, pattern = (list(t) for t in zip(*sorted(zip(support, pattern),reverse=True)))
        pattern = pattern[:n]
        support = support[:n]

    frequent_pattern = {key : supp for key, supp in zip(pattern, support)}
    return frequent_pattern

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

def extract_feature(sequence, patterns):
    ''' Return the generalized transition feature which is a binary value that 1.0 for exisited pattern 0.0 for non-exsisted pattern.
    :params str sequence: A string sequence to be extracted feature from
    :params list patterns: A list of string patterns.
    :return: A dictionary which key is the pattern and value is 1.0 or 0.0.
    :rtype: dictionary
    '''
    pattern_found = []
    for p in patterns:
        if find_pattern(p, sequence):
            pattern_found.append(1.0)
        else:
            pattern_found.append(0.0)

    patterns = [sequence_viz(p) for p in patterns]
    feature = {key: f for key, f in zip(patterns, pattern_found)}
    return feature
