import sys
import csv
import getopt
import hashlib
import argparse
from dateutil.parser import parse
from pandas import *

'''
Each system that will send us data directly can use a script to anonymize the
relevant columns. Each system will likely store data in slightly different
formats, so we can hash several IDs and link on whatever's common between them:

- complete SSN
- Last four of SSN
- Last name
- First name

We need to take a couple extra steps to ensure consistency:

- We should salt each of the IDs. A salt is a random string of characters that
  gets added to the ID, e.g. the salt might be '13f1d56a' and the SSN might be
  123456789. Then we'd hash '13f1d56a123456789'. The salt helps prevent
  someone from hashing all 1,000,000,000 available SSNs.

- Would Steve mind creating a salt and sharing it with everyone who will send
  us data? That way Johnson County and the police departments can de-anonymize
  identities but no one else can. (Sorry for volunteering you, Steve! You can
  do it in SQL Server with

   select substring(HashBytes('MD5', 'string of your choice'), 8), :sunglasses:

- We should convert names to uppercase and remove all characters outside [A-Z]
'''
class WrongArgs(Exception):
    def __init__(self, error_message):
        self.error_message = error_message
    def __str__(self):
        return repr(self.error_message) 

def cmd_args_parser():
    args_dict = {}

    parser = argparse.ArgumentParser()
    parser.add_argument("--ifile", help="xlxs file you want to hash")
    parser.add_argument("--full_ssn", help="column in file that contains full ssn")
    parser.add_argument("--last_4_ssn", help = "column in file that contains last four digits for ssn")
    parser.add_argument("--fname", help="column in file that contains first name")
    parser.add_argument("--lname", help = "column in file that contains last name")
    parser.add_argument("--salt", help="please enter input salt string for hashing")
    parser.add_argument("--ofile", help="enter name of file you want to output to")

    args = parser.parse_args()

    if args.ifile:
        args_dict['ifile'] = args.ifile
    else:
        raise WrongArgs("Wrong cmd input! example python hasher.py --ifile example_file.xlsx")

    if args.salt:
        args_dict['salt'] = args.salt
    else:
        raise WrongArgs('Wrong cmd input!  you need to add a salt string example python hasher.py --file example_file.xlsx --salt "salt_string_example"')

    if args.ofile:
        args_dict['ofile'] = args.ofile
    else:
        raise WrongArgs("need to input output file as --ofile example_output_file.xlsx")

    if args.full_ssn:
        args_dict['full_ssn'] = args.full_ssn
    if args.last_4_ssn:
        args_dict['last_4_ssn'] = args.last_4_ssn
    if args.fname:
        args_dict['fname']= args.fname
    if args.lname:
        args_dict['lname'] = args.lname

    if len(args_dict.keys()) < 3:
        raise WrongArgs(
            "need to input a column to hash.  options are : --full_ssn , --last_4_ssn, --fname, --lname")

    return args_dict

########################
# cleaning methods     #
########################

def full_ssn_clean(ssn):
    cleaned_ssn = str(ssn).replace('-','').strip()
    error = not (len(cleaned_ssn) == 9 and cleaned_ssn.isdigit())
    return cleaned_ssn, error

def last_4_ssn_clean(last_4_ssn):
    ssn_str = str(last_4_ssn).strip()
    error = not (len(ssn_str) == 4 and ssn_str.isdigit())
    return ssn_str, error

# Create a string that contains all non-uppercase ACSII
ASCII_TO_INGORE = ''.join(chr(i) for i in xrange(128) if i not in xrange(ord('A'),ord('Z')+1))
def name_clean(name):
    error = False
    try:
        name = name.encode('ascii','ignore') # drop all non-ascii characters
    except:
        name = str(name)
        error = True
    clean_name = name.upper().translate(None, ASCII_TO_INGORE)
    return clean_name, (error or len(clean_name) == 0)

def extract_last_4_ssn(ssn):
    ssn = str(ssn)
    if len(ssn) > 3:
        return ssn[-4:], (not ssn.isdigit())
    else:
        return ssn, True

###########################
#      Hashing Methods    #
###########################


def hash_full_ssn(ssn_col, salt, reader):
    for (idx, row) in reader.iterrows():
        cleaned_ssn, error = full_ssn_clean(row[ssn_col])
        reader.ix[idx, 'hashed_full_ssn'] = hashlib.sha256(salt+cleaned_ssn).hexdigest()
        reader.ix[idx, 'full_ssn_error'] = error

        last_4_ssn, error_l4 = extract_last_4_ssn(cleaned_ssn)
        reader.ix[idx, 'hashed_last_4_ssn'] = hashlib.sha256(salt+last_4_ssn).hexdigest()
        reader.ix[idx, 'last_4_ssn_error'] = error_l4

    return reader

def hash_last_4_ssn(last_4_ssn, salt, reader):
    for (idx, row) in reader.iterrows():
        cleaned_last_4_ssn, error_last_4_ssn = last_4_ssn_clean(row[last_4_ssn])

        reader.ix[idx, 'hashed_last_4_ssn'] = hashlib.sha256(salt+cleaned_last_4_ssn).hexdigest()
        reader.ix[idx, 'last_4_ssn_error'] = error_last_4_ssn

    return reader

def hash_name(name, salt, reader):
    for (idx, row) in reader.iterrows():
        name_cleaned, error_name = name_clean(row[name])

        reader.ix[idx, 'hashed_' + name] = hashlib.sha256(salt+name_cleaned).hexdigest()
        reader.ix[idx, name + '_error'] = error_name

    return reader


def main():
    args_dict = cmd_args_parser()
    

    xls = ExcelFile(args_dict['ifile'])
    out = ExcelWriter(args_dict['ofile'])
    for sheet in xls.sheet_names:
        df = xls.parse(sheet)

        if 'full_ssn' in args_dict and args_dict['full_ssn'] in df.keys():
            hash_full_ssn(args_dict['full_ssn'], args_dict['salt'], df)
            df.pop(args_dict['full_ssn'])

        if 'last_4_ssn' in args_dict and args_dict['last_4_ssn'] in df.keys():
            hash_last_4_ssn(args_dict['last_4_ssn'], args['salt'], df)
            df.pop(args_dict['last_4_ssn'])

        if 'lname' in args_dict and args_dict['lname'] in df.keys():
            hash_name(args_dict['lname'], args_dict['salt'], df)
            df.pop(args_dict['lname'])

        if 'fname' in args_dict and args_dict['fname'] in df.keys():
            hash_name(args_dict['fname'], args_dict['salt'], df)
            df.pop(args_dict['fname'])

        df.to_excel(out, sheet_name=sheet, index=False)

if __name__ == "__main__":
    main()
