from .. import setup_environment
import pandas as pd
import numpy as np
import json
from itertools import combinations
from random import shuffle
from . import models
import subprocess
import datetime
import pickle
import boto3
from optparse import OptionParser
import os
import tables
import pdb
import yaml
import uuid
import botocore

engine, config_db = setup_environment.get_database('pipeline/default_profile.yaml')
try:
    con = engine.raw_connection()
    # missing schema information ..
except:
    # change to log statement
    print 'cannot connect to database'


class FeatureModelGrabber():

    def __init__(self, fake_today, prediction_window, config,
        feature_timestamp, s3_profile, discard_model):
        self.fake_today = fake_today
        self.prediction_window = prediction_window
        self.feature_timestamp = feature_timestamp
        self.config = config
        self.s3_profile = s3_profile
        self.discard_model = discard_model
        self.batch_timestamp = datetime.datetime.now().isoformat()
        self.results_directory = os.path.abspath(("{}_{}").format(self.config['directory'],
                                                  self.batch_timestamp))

    def load_table(self, train_or_test, feature_timestamp):
        # get feature table name
        if feature_timestamp == '%':
            feature_timestamp = pd.read_sql('''
                SELECT
                    split_part(table_name, '_at_', 2)
                FROM
                    information_schema.tables
                WHERE
                    table_schema = 'feature_tables'
                ORDER BY
                    1 desc
                LIMIT
                    1;
            ''', con).iat[0,0]
        feature_table_name = ('{}."features_{}_{}_{}_at_{}"').format(config_db['feature_schema'],
                train_or_test, self.fake_today, self.prediction_window,
                feature_timestamp)

        # load table
        print 'loading {}'.format(feature_table_name)
        query = ('SELECT * FROM {}').format(feature_table_name)
        full_feature_table = pd.read_sql(query, con = con)
        return full_feature_table, feature_table_name

    def load_train_table(self):
        full_feature_table = self.load_table("train", self.feature_timestamp)
        return full_feature_table

    def load_test_table(self):
        full_feature_table = self.load_table("test", self.feature_timestamp)
        return full_feature_table

    def load_feature_name_dictionary(self):
        feature_dictionary_filename = ('pipeline/classmap_dictionaries/classmap_{}_{}.json').format(
            self.fake_today, self.prediction_window)
        with open(feature_dictionary_filename, 'r') as f:
            feature_names_dict = json.load(f)
        return feature_names_dict

    def get_feature_sets(self, feature_names_dict):
        feature_sets_to_test = []
        for feature_group in self.config['feature_groups']:
            feature_table_col_names = []
            for feature_name in self.config['features'][feature_group].keys():
                if self.config['features'][feature_group][feature_name]:
                    if feature_name.lower() in feature_names_dict:
                        feature_table_col_names.extend(feature_names_dict[feature_name.lower()])

            feature_sets_to_test.append(
                    {'feature_groups' : [feature_group],
                    'feature_column_names' : feature_table_col_names})
        return feature_sets_to_test

    def generate_feature_group_combinations(self, feature_groups):
        combination_unflattened = sum([map(list,
                                      combinations(feature_groups, i)) for i in range(len(feature_groups) + 1)],
                                      [])
        
        combinations_flattened = []
        for combination in combination_unflattened:
            flattened_combination = {'feature_column_names':[],
                                            'feature_groups': []}
            for feature_group in combination:
                flattened_combination['feature_column_names'].extend(
                        feature_group['feature_column_names'])
                flattened_combination['feature_groups'].extend(
                        feature_group['feature_groups'])
            combinations_flattened.append(flattened_combination)
        return combinations_flattened[1:]

    def extract_train_x(self, feature_set, full_feature_table):
        pass
        '''feature_set.append('personid')
        feature_set = feature_set[]'''

    def add_labels_to_feature_sets(self, feature_sets, labels):
        print labels
        feature_sets_copy = []
        for feature_set in feature_sets:
            for label in labels:
                feature_set_copy = feature_set.copy()
                feature_set_copy['label'] = label
                feature_sets_copy.append(feature_set_copy)
        return feature_sets_copy

    def parameter_generator(self, params_lst):
        if params_lst == []:
            return [{}]
        else:
            combo_lst = self.parameter_generator(params_lst[1:])
            updated_combo_list=[]
            for val in params_lst[0][params_lst[0].keys()[0]]:
                for dic in combo_lst:
                    dictionary_copy = dic.copy()
                    dictionary_copy[params_lst[0].keys()[0]] = val
                    updated_combo_list.append(dictionary_copy)
            return updated_combo_list

    def generate_model_parameter_list(self):
        models_lst = []
        #lst = self.parameter_generator([{'C_reg':[100,4,5]},{'pen':[5,3,4]}])
        for model in self.config['model']:
            possible_args_lst = []
            if model not in self.config['parameters']:
                model_params_lst = [{'model': model, 'model_params': {}}]
            else:
                for key,val in self.config['parameters'][model].items():
                    temp = {key:val}
                    possible_args_lst.append(temp)
                full_params_lst = self.parameter_generator(possible_args_lst)
                model_params_lst = [{'model': model, 'model_params':x} for x in full_params_lst]
            models_lst.extend(model_params_lst)
        return models_lst
              
    def combine_models_labels_features(self, models, labelled_features):
        combined_lst = []
        for model in models:
            #print model
            for feature_set in labelled_features:
                combined_sets = feature_set.copy()
                combined_sets.update(model)
                combined_lst.append(combined_sets)
        return combined_lst

    def generate_feature_group(self, feature_sets):
        feature_set = {'feature_column_names': []}
        for feat_set in feature_sets:
            feature_set['feature_column_names'].extend(feat_set['feature_column_names'])
        return [feature_set]

    def export_data_table(self, table, end_date, label, feature_names):
        """ Save a data set as an HDF table for later reuse.

        :param table: the DataFrame to save
        :type table: pandas DataFrame
        :param end_date: end of labeling period
        :type end_date: a date format of some kind
        :param label: name of the column containing labels
        :type label: str
        :param feature_names: names of the columns containing features
        :type feature_names: list
        :return: the prefix of the HDF filename
        :rtype: str
        """
        if type(end_date) == np.datetime64:
            end_date = np.datetime_as_string(end_date,
                                             timezone = 'local')[:10]
        else:
            end_date = end_date.to_datetime().date().isoformat()

        file_name = self.export_metadata(end_date, label, feature_names)
        file_path = '{0}/{1}.h5'.format(self.results_directory, file_name)

        if not os.path.exists(file_path):
            store = pd.HDFStore(file_path)
            store['df'] = table
            store.close()

        self.upload_file_to_s3('{0}.h5'.format(file_name), 'hdf_bucket_name',
                               file_path)

        print("uploaded hdf to s3")

        return(file_name)

    def upload_file_to_s3(self, key_name, bucket, local_file_path):
        """
        """
        s3, s3_config = self.connect_to_s3()
        key_name = '{0}/{1}'.format(s3_config['folder'], key_name)
        s3.Object(s3_config[bucket], key_name).upload_file(local_file_path)
        os.remove(local_file_path)

    def export_metadata(self, end_date, label, feature_names):
        """ Construct and export metadata for a matrix. Return a unique
        identifier based on this metadata to used as a filename.

        :param end_date: the end date of the labeling period for the matrix
        :type end_date: str
        :param label: name of the column containing labels
        :type label: str
        :param feature_names: names of the columns containing features
        :type feature_names: list
        :return: unique identifier for the matrix
        :rtype: str
        """
        metadata = {'start_date': str(datetime.datetime.strptime(self.config['feature_start_date'],
                                      "%d%b%Y").date()),
                    'end_date': end_date,
                    'prediction_window': self.prediction_window,
                    'unit_id': config_db['id_column'],
                    'labelname': label,
                    'labeltype': self.config['label_types'][label],
                    'feature_names': sorted(feature_names),
                    'data_id' : 'johnson_county_ddj'}
        
        file_name = self.generate_uuid(metadata)
        file_path = '{0}/{1}.yaml'.format(self.results_directory, file_name)
        
        with open(file_path, 'w') as outfile:
            yaml.dump(metadata, outfile, default_flow_style = True)

        self.upload_file_to_s3('{0}.yaml'.format(file_name), 'hdf_bucket_name',
                               file_path)

        return file_name

    def generate_uuid(self, metadata):
        """ Generate a unique identifier given a dictionary of matrix metadata.

        :param metadata: metadata for the matrix
        :type metadata: dict
        :return: unique name for the file
        :rtype: str
        """
        identifier = ''
        for key in sorted(metadata.keys()):
            identifier = '{0}_{1}'.format(identifier, str(metadata[key]))
        name_uuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, identifier))
        return  name_uuid

    def run(self, labels):
        if not os.path.exists(self.results_directory):
            os.makedirs(self.results_directory)

        # get data
        train_table, train_table_name = self.load_train_table()
        test_table, test_table_name = self.load_test_table()

        # map column names to feature names and feature groups
        feature_name_dict = self.load_feature_name_dictionary()
        feature_groups = self.get_feature_sets(feature_name_dict)

        # if testing feature group combinations, generate new groups
        if self.config['test_feature_group_combinations']:
            feature_sets = self.generate_feature_group_combinations(feature_groups)
        else:
            feature_sets = self.generate_feature_group(feature_groups)
    
        # make the list of columns to use in each model
        labelled_feature_sets = self.add_labels_to_feature_sets(feature_sets,
                                                                labels)

        # make the list of modeling jobs to run
        model_combinations = self.generate_model_parameter_list()
        all_experiments = self.combine_models_labels_features(model_combinations,
                                                              labelled_feature_sets)
        bulk_model_list = []
        print ("total amount of exps: {}").format(len(all_experiments))
        if self.config["randomize_model_run_order"] == True:
            shuffle(all_experiments)

        # initialize DataFrame of train-test combinations
        train_test_combos = []
        
        # Is this the first run for this label & prediction window combination?
        first_runs = {}
        for label in labels:
            for window in self.config['prediction_window']:
                for feature_list in pd.DataFrame(all_experiments)['feature_column_names']:
                    first_runs[label + str(window) +
                               ''.join(sorted(feature_list))] = True

        # run the models
        for idx,exp in enumerate(all_experiments):
            print ("{}/{} : {}").format(idx, len(all_experiments), exp['model'])
            window_start_dates = train_table['training_end_date'].sort_values().unique()
            for train_start, test_start in self.iterate_train_test(window_start_dates):
                print train_start, test_start
                
                # make train-test split
                train_data = train_table.loc[train_table['training_end_date'] == train_start]
                if test_start is not None:
                    test_data = train_table.loc[train_table['training_end_date'] == test_start]
                    this_test_table_name = train_table_name
                else:
                    test_data = test_table
                    test_start = test_data['training_end_date'][0]
                    this_test_table_name = test_table_name
                
                model = models.Model(exp['model'], exp['model_params'],
                                     exp['label'], train_data, test_data,
                                     exp['feature_column_names'], self.config)

                train_matrix = pd.concat(model.get_training_data(), axis = 1)
                text_matrix = pd.concat(model.get_test_data(), axis = 1)

                # if the data have not been saved yet, save them
                # if self.discard_model == False:
                if first_runs[exp['label'] + str(self.prediction_window) + 
                                ''.join(sorted(exp['feature_column_names']))]:
                    train_file_name = self.export_data_table(train_matrix,
                                                             train_data['labeling_end_date'].unique()[0],
                                                             exp['label'],
                                                             exp['feature_column_names'])
                    test_file_name = self.export_data_table(text_matrix,
                                                            test_data['labeling_end_date'].unique()[0],
                                                            exp['label'],
                                                            exp['feature_column_names'])
                    train_test_combos.append({'train' : '{0}.h5'.format(train_file_name),
                                              'test' : '{0}.h5'.format(train_file_name)})

                res_dict , clf = model.run()
                res_dict['fake_today'] = test_start
                res_dict['prediction_window'] = self.prediction_window
                res_dict['train_table_name'] = train_table_name
                res_dict['test_table_name'] = this_test_table_name
                res_dict['batch_timestamp'] = self.batch_timestamp
                res_dict['unique_timestamp'] = datetime.datetime.now().isoformat()
                res_dict['feature_group_column'] = feature_sets,
                res_dict['model_params'] = exp['model_params']
                #res_dict['feature_groups'] = feature_sets.keys()


                pkl_filename = self.pickle_results(res_dict, clf)
                res_dict['pkl_file'] = pkl_filename

                bulk_model_list = self.compile_results(res_dict,
                                                       bulk_model_list)
            first_runs[exp['label'] + str(self.prediction_window) + 
                       ''.join(sorted(exp['feature_column_names']))] = False

        self.write_matrix_pairs(train_test_combos)
        self.compile_results(None, bulk_model_list, force_write = True)
        # os.rmdir(self.results_directory)

    def write_matrix_pairs(self, train_test_combos):
        """ Given a list of train-test pairs, write them locally, check s3
        for an existing set, combine the sets, remove duplicates, and upload
        new copy to s3.

        :param train_test_combos: list of dictionaries with keys 'train' and
                                  'test' with filenames of HDF matrices as 
                                  values
        :type train_test_combos: list
        :return: None
        :rtype: None
        """
        # write pairs from this session to new file
        matrix_pairs = pd.DataFrame(train_test_combos) 
        self.write_to_csv(matrix_pairs, ['train','test'],
                          'matrix_pairs.txt')

        # check s3 for old file
        s3, s3_config = self.connect_to_s3()
        old_file_name = "{}/old_matrix_pairs.txt".format(self.results_directory)
        key_name = '{}/matrix_pairs.txt'.format(s3_config['folder'])
        try:
            s3.Bucket(s3_config['hdf_bucket_name']).download_file(key_name,
                                                                  old_file_name)
        except Exception: 
            pass

        # if an old file found, combine them and remove duplicates
        if os.path.isfile(old_file_name):
            abs_fname = self.results_directory + '/matrix_pairs.txt'
            cmd = ('cat {} {} | sort -u > tmp.txt;'
                   'mv -v tmp.txt {}; rm {}'.format(abs_fname,
                                                    old_file_name,
                                                    abs_fname,
                                                    old_file_name))
            os.system(cmd)

        # upload final file to s3
        self.upload_file_to_s3('matrix_pairs.txt', 'hdf_bucket_name',
                               '{0}/matrix_pairs.txt'.format(self.results_directory))

    def iterate_train_test(self, iterable):
        """ Iterate over prediction window start dates, returning the start
        dates for train and test data for the current model.

        :param prediction_window_start_dates: list of prediction window start
                                              dates
        :type: list
        :return: train date and test date
        :rtype:
        """
        iterator = iter(iterable)
        current_item = next(iterator)  # throws StopIteration if empty.
        for next_item in iterator:
            yield (current_item, next_item)
            current_item = next_item
        yield (current_item, None)

    def connect_to_s3(self):
        """ Open a connection to s3 and return the resource  objects and a
        dictionary of s3 configuration details.

        :return: s3 resource and s3_config
        :rtype: boto3 resource and dict
        """
        with open(self.s3_profile) as g:
            s3_config = json.load(g)

        s3 = boto3.resource('s3', aws_access_key_id = s3_config['key'],
                            aws_secret_access_key = s3_config['secret_key'],
                            region_name = s3_config['region'])
        
        return(s3, s3_config)

    def pickle_results(self, res_dict, clf):
        """ Pickle the model object locally, upload to s3, and delete local copy

        :param self: inherit object properties
        :type self: FeatureModelGrabber
        :param res_dict: dictionary of model information
        :type res_dict: dict
        :param clf: model object
        :type clf: model
        :return: path to pickle file
        :rtype: str
        """
        pkl_file = ('{}/{}_{}.pkl').format(self.results_directory,
                                           self.config['pkl_prefix'],
                                           res_dict['unique_timestamp'])
        
        # open s3 connection
        s3, s3_config = self.connect_to_s3()

        s3_key_name = '{0}/{1}'.format(s3_config['folder'], pkl_file)

        # dump model results into a pickle and upload to s3
        with open(pkl_file, 'wb') as f:
            pickle.dump(clf, f, protocol = pickle.HIGHEST_PROTOCOL)

        self.upload_file_to_s3(pkl_file, 'pickle_bucket_name', pkl_file)
        print("uploaded pickle to S3.")

        return pkl_file

    def compile_results(self, res, bulk_model_list, force_write = False):
        """ After a model is run, compile the model information and the
        predictions. Temporarily stash them in csvs. If  more than 49 models
        have been stashed or this is the last model to be run, copy the csvs
        to the models and predictions table, remove the csvs, and return an
        empty list.

        :param self: inherit object properties
        :type self: FeatureModelGrabber
        :param res: dictionary of model information
        :type res: dict
        :param bulk_model_list: list of model info to be saved to database
        :type bulk_model_list: list
        :param force_write: should the stashed info be saved to the database
                            regardless of length?
        :type force_write: bool
        :return: list of models run since last write
        :rtype: list
        """
        
        if res != None:
            # compile model info
            model_dict = {'unique_timestamp': res['unique_timestamp'],
                          'batch_timestamp': res['batch_timestamp'],
                          'model_name': res['model_name'],
                          'labelling': res['label'],
                          'config':  json.dumps(res['config']),
                          'fake_today': res['fake_today'],
                          'prediction_window': res['prediction_window'],
                          'filename': res['pkl_file'],
                          'model_params': json.dumps(res['model_params']),
                          'feature_importance': res['feature_importance'],
                          'columns_for_feat_importance': res['columned_used_for_feat_importance'],
                          'train_table_name': res['train_table_name'],
                          'test_table_name': res['test_table_name'],
                          'head_hash': subprocess.check_output(['git', 
                                                               'rev-parse', 
                                                               'HEAD']).rstrip(),
                          'discard_model': self.discard_model}
            model_list = [model_dict]
            model_df = pd.DataFrame(model_list)
            model_columns = ['batch_timestamp', 'columns_for_feat_importance',
                             'config', 'fake_today', 'feature_importance',
                             'filename', 'labelling', 'model_name',
                             'model_params', 'prediction_window',
                             'unique_timestamp', 'train_table_name',
                             'test_table_name', 'head_hash', 'discard_model']
            print("writing model info to csv")
            self.write_to_csv(model_df, model_columns, 'models.csv')
            bulk_model_list.append(model_dict)

            # compile predictions
            prediction_lst = []
            for idx, id_ in enumerate(res['test_ids']):
                prediction_info= {'label': res['test_y'][idx],
                                  config_db['id_column']: id_,
                                  'prediction_prob': res['prob_prediction_test_y'][idx],
                                  'unique_timestamp': res['unique_timestamp']}
                prediction_lst.append(prediction_info)
            prediction_df = pd.DataFrame(prediction_lst)
            prediction_cols = ['label', config_db['id_column'],
                               'prediction_prob',
                               'unique_timestamp']
            print("writing predictions to csv")
            self.write_to_csv(prediction_df, prediction_cols, 'predictions.csv')

        # if list of models since last write contains more than 49 models, or
        # it's the last model, write to db
        if len(bulk_model_list) > 49 or force_write == True:
            print 'writing {0} model(s) to output'.format(len(bulk_model_list))
            self.csv_to_database('predictions.csv', config_db['predictions'])
            self.csv_to_database('models.csv', config_db['models'])
            bulk_model_list = []
        
        return bulk_model_list

    def write_to_csv(self, df, column_order, file_name):
        """ Given a dataframe, a specific column order, and a csv filename,
        enforce the column order on the dataframe and then append the data to
        the specified csv file.

        :param self: inherit object properties
        :type self: FeatureModelGrabber
        :param df: data output by the modeling process containing either model
                   information or predictions
        :type res: pandas DataFrame
        :param column_order: the order of columns in the relevant database table
        :type column_order: list
        :param file_name: the name of the csv file to write to
        :type file_name: str
        :return: None
        :rtype: None
        """
        df = df[column_order]
        with open('{0}/{1}'.format(self.results_directory,
                  file_name), 'a+') as f:
            df.to_csv(f, header = False, index = False)
        
    def csv_to_database(self, file_name, table_name):
        """ Given a csv name and a database table name, append the contents of
        the csv to the database table and remove the csv.

        :param file_name: name of csv to save to database
        :type file_name: str
        :param table_name: name of the database table to copy to
        :type table_name: str
        :return: None
        :rtype: None
        """
        # set up psql environment variables
        with open(config_db['db_connection_config_path']) as f:
            db_vals = json.load(f)
        for k,v in db_vals.items():
            os.environ['PG' + k.upper()] = str(v) if v else ""

        # copy file contents to table
        os.system("""
                    cat {0}/{1} |
                    psql -c "COPY output.{2} FROM stdin with csv;"
                  """.format(self.results_directory, file_name, table_name))
        
        # remove file
        os.remove('{0}/{1}'.format(self.results_directory, file_name))


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def write_dataframe_chunks(df_name, df):
    for df_chunk in chunker(df, 100000):
        df_chunk.to_sql(df_name, engine, if_exists='append', index = False,
                        schema = 'output', chunksize= 5000)
        print 'wrote chunk'
