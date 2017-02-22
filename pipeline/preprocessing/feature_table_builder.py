import datetime
import logging
import pandas as pd
from ..queries import timeframe_queries, basicqueries
from .. import setup_environment
from . import feature_processor
import json
import pdb
import cStringIO

engine, config_db = setup_environment.get_database('pipeline/default_profile.yaml')
try:
    con = engine.raw_connection()
    # missing schema information ..
except:
    # change to log statement
    print 'cannot connect to database'



class Labeller():
    def __init__(self, start_date, end_date, labels):
        self.start_date = start_date
        self.end_date = end_date
        self.labels = labels

    def get_labels(self):
        timeframe_query_training = timeframe_queries.timeframe_table_end_date(
            config_db['personid_event_dates'], self.start_date, 'begin_date')

        people_query_training = basicqueries.count_vals_column_for_id(
            timeframe_query_training, config_db["id_column"], "event",
            ['ems', 'mh', 'booking'])

        people_training =  pd.read_sql(people_query_training, con = con)


        timeframe_query_labels = timeframe_queries.timeframe_table_start_end_date(
            config_db['personid_event_dates'], self.start_date, self.end_date,
            'begin_date', 'begin_date')

        people_query_labels = basicqueries.count_vals_column_for_id(
            timeframe_query_labels, config_db["id_column"], "event",
            ['ems', 'mh', 'booking'])

        people_labelling = pd.read_sql(people_query_labels, con = con)

        people = people_training.merge(people_labelling,
                                       on = config_db["id_column"], how ='left')

        people = people.fillna(0)
        people.drop_duplicates(inplace = True)


        labelling_columns = [config_db["id_column"]]

        if 'ems_jims' in self.labels:
            ems_jims = ('{}').format('ems_jims')
            people[ems_jims] = None
            people[ems_jims][(people['booking_sum_x'] == 0)& (people['ems_sum_x'] > 0) 
                                    & (people['booking_sum_y'] > 0) & (people['mh_sum_x'] == 0)] = 1
            people[ems_jims][(people['booking_sum_x'] == 0)& (people['ems_sum_x'] > 0) 
                                    & (people['booking_sum_y'] == 0) & (people['mh_sum_x'] == 0)] = 0
            labelling_columns.append(ems_jims)
        if 'mh_jims' in self.labels:
            mh_jims = ('{}').format('mh_jims')
            people[mh_jims] = None
            people[mh_jims][(people['mh_sum_x'] > 0) & (people['booking_sum_y'] > 0) 
                                    & (people['booking_sum_x'] == 0) & (people['ems_sum_x'] == 0)] = 1
            people[mh_jims][(people['mh_sum_x'] > 0) & (people['booking_sum_y'] == 0) 
                                    & (people['booking_sum_x'] == 0) & (people['ems_sum_x'] == 0)] = 0
            labelling_columns.append(mh_jims)
        if 'ems_mh_jims' in self.labels:
            ems_mh_jims = ('{}').format('ems_mh_jims')
            people[ems_mh_jims] = None
            people[ems_mh_jims][(people['mh_sum_x'] > 0) &
                                    (people['booking_sum_y'] > 0) & (people['ems_sum_x'] >0) & (people['booking_sum_x'] == 0)] =1
            people[ems_mh_jims][(people['mh_sum_x'] > 0) &
                                    (people['booking_sum_y'] == 0) & (people['ems_sum_x'] >0) & (people['booking_sum_x'] == 0)] =0
            labelling_columns.append(ems_mh_jims)
        if 'ems_jims_mh' in self.labels:
            ems_jims_mh = ('{}').format('ems_jims_mh')
            people[ems_jims_mh] = None
            people[ems_jims_mh][(people['booking_sum_x'] > 0) &
                                    (people['mh_sum_y'] > 0) & (people['ems_sum_x'] >0) & (people['mh_sum_x'] == 0)] =1
            people[ems_jims_mh][(people['booking_sum_x'] > 0) &
                                    (people['mh_sum_y'] == 0) & (people['ems_sum_x'] >0) & (people['mh_sum_x'] == 0)] =0
            labelling_columns.append(ems_jims_mh)
        if 'ems_ems' in self.labels:
            ems_ems = ('{}').format('ems_ems')
            people[ems_ems] = None
            people[ems_ems][(people['mh_sum_x'] == 0) & (people['ems_sum_x'] > 0) &
                                    (people['booking_sum_x'] == 0) & (people['ems_sum_y'] >0)] =1
            people[ems_ems][(people['mh_sum_x'] == 0) & (people['ems_sum_x'] > 0) &
                                    (people['booking_sum_x'] == 0) & (people['ems_sum_y'] ==0)] =0
            labelling_columns.append(ems_ems)
        if 'mh_ems' in self.labels:
            mh_ems = ('{}').format('mh_ems')
            people[mh_ems] = None
            people[mh_ems][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] == 0) &
                                    (people['booking_sum_x'] == 0) & (people['ems_sum_y'] >0)] =1
            people[mh_ems][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] == 0) &
                                    (people['booking_sum_x'] == 0) & (people['ems_sum_y'] ==0)] =0
            labelling_columns.append(mh_ems)
        if 'ems_mh' in self.labels:
            ems_mh = ('{}').format('ems_mh')
            people[ems_mh] = None
            people[ems_mh][(people['mh_sum_x'] == 0) & (people['ems_sum_x'] > 0) &
                                    (people['booking_sum_x'] == 0) & (people['mh_sum_y'] >0)] =1
            people[ems_mh][(people['mh_sum_x'] == 0) & (people['ems_sum_x'] > 0) &
                                    (people['booking_sum_x'] == 0) & (people['mh_sum_y'] ==0)] =0
            labelling_columns.append(ems_mh)
        if 'jims_mh_jims' in self.labels:
            jims_mh_jims = ('{}').format('jims_mh_jims')
            people[jims_mh_jims] = None
            people[jims_mh_jims][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] == 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] >0)] =1
            people[jims_mh_jims][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] == 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] ==0)] =0
            labelling_columns.append(jims_mh_jims)
        if 'jims_jims' in self.labels:
            jims_jims = ('{}').format('jims_jims')
            people[jims_jims] = None
            people[jims_jims][(people['mh_sum_x'] == 0) & (people['ems_sum_x'] == 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] >0)] =1
            people[jims_jims][(people['mh_sum_x'] == 0) & (people['ems_sum_x'] == 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] ==0)] =0
            labelling_columns.append(jims_jims)
        if 'anything_jims' in self.labels:
            anything_jims = ('{}').format('anything_jims')
            people[anything_jims] = None
            people[anything_jims][(people['booking_sum_y'] >0)] =1
            people[anything_jims][(people['booking_sum_y'] ==0)] =0
            labelling_columns.append(anything_jims)
        if 'jims_mh__ems___jims' in self.labels:
            jims_mh__ems___jims = ('{}').format('jims_mh__ems___jims')
            people[jims_mh__ems___jims] = None
            people[jims_mh__ems___jims][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] >= 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] > 0)] =1
            people[jims_mh__ems___jims][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] >= 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] == 0)] = 0
        if 'jims_mh_ems_jims' in self.labels:
            jims_mh_ems_jims = ('{}').format('jims_mh_ems_jims')
            people[jims_mh_ems_jims] = None
            people[jims_mh_ems_jims][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] > 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] >0)] =1
            people[jims_mh_ems_jims][(people['mh_sum_x'] > 0) & (people['ems_sum_x'] > 0) &
                                    (people['booking_sum_x'] > 0) & (people['booking_sum_y'] ==0)] =0


        outcomes = people[labelling_columns].convert_objects(convert_numeric=True)
        outcomes.fillna(0)

        return outcomes

def generate_fake_todays(fake_today, prediction_window, start_date):
    ''' Given a final prediction window start date, the length of the prediction
    windows, and a training start date, return the start and end dates for all
    prediction windows as a dictionary.

    :param fake_today: start date for the final prediction window
    :type fake_today: datetime
    :param prediction_window: length of the prediction windows in days
    :type prediction_window: int
    :param start_date: start date for the training period
    :type start_date: datetime
    :returns: start and end dates for all prediction windows
    :rtype: dict
    '''
    fake_today_times = []
    temp_fake_today = fake_today
    train_end_date = temp_fake_today - datetime.timedelta(days = prediction_window)
    while train_end_date > start_date:
        fake_today_times.append({'start_date_labelling': train_end_date,
                                 'end_date_labelling': temp_fake_today})
        temp_fake_today = train_end_date
        train_end_date = train_end_date - datetime.timedelta(days = prediction_window)
    return fake_today_times


def label_feature_producer(start_date, end_date, features, labels):
    labeller = Labeller(start_date, end_date, labels)
    dataset = labeller.get_labels()
    dataset['training_end_date'] = start_date
    dataset['labeling_end_date'] = end_date
    # dataset
    feature_grabber = feature_processor.FeatureGrabber(start_date, engine,config_db, con)
    feature_name_dictionary = {}

    #print dataset
    for feature in features:
            #print feature
        res_training, feature_names_training = feature_grabber.getFeature(feature)
        feature_name_dictionary[feature] = feature_names_training
        res_training.drop_duplicates(inplace = True)
        dataset = pd.merge(dataset, res_training, on = config_db['id_column'],
                           how = 'left')

    return dataset, feature_name_dictionary

    #return None, None


def generate_feature_list(config):
    feature_list = []
    feature_groups = config['feature_groups']

    for feature_group in feature_groups:
        feature_group_dict = config['features'][feature_group]
        for feature in feature_group_dict:
            if feature_group_dict[feature]:
                feature_list.append(feature)

    return feature_list

def merge_feature_dictionaries(d1, d2):
    fin_dic = {}
    for k in d2.iterkeys():
        fin_dic[k] = list(set(d1.get(k, [])) | set(d2.get(k, [])))
    return fin_dic

def dataframe_merge(d1, d2):
    d1_columns = list(d1.columns)
    #print d1_columns
    d2_columns = list(d2.columns)

    if len(d2_columns) ==0:
        return d1
    columns_missing_in_d2 = list((d1_columns) - (d2_columns))
    print columns_missing_in_d2

    for column in columns_missing_in_d2:
        if column not in d2_columns:
            print 'adding to d2'
            d2[column] = None

    columns_missing_in_d1 = list(set(d2_columns) - set(d1_columns))
    print columns_missing_in_d1

    for column in columns_missing_in_d1:
        if column not in d1_columns:
            print 'adding to d1'
            d1[column] = None

    #for idx, col in enumerate(d1)
    print d1_columns == d2_columns

    merged_df = pd.concat([d1,d2], join = 'outer')
    return merged_df

def generate_feature_table(config, fake_today, prediction_window, start_date,
                           feature_timestamp):
    fake_today_dt = datetime.datetime.strptime(fake_today, "%d%b%Y")
    start_date_dt = datetime.datetime.strptime(start_date, "%d%b%Y")

    logging.debug("generate labels")

    labels = []
    for label in config['labelling'].keys():
        if config['labelling'][label]:
            labels.append(label)

    logging.debug(labels)

    feature_list = generate_feature_list(config)

    dates_for_fake_today = generate_fake_todays(fake_today_dt,
                                                prediction_window,
                                                start_date_dt)

    #for train_x_set
    print 'building training set'

    feature_col_dict = {}
    train_dataset = pd.DataFrame()
    for date_info in dates_for_fake_today:
        print date_info
        dataset, features = label_feature_producer(date_info['start_date_labelling'],
                                                   date_info['end_date_labelling'],
                                                   feature_list, labels)
        dataset.drop_duplicates(inplace = True)
        train_dataset = pd.concat([dataset, train_dataset], copy=False)
        #if len(dataset.columns)
        #train_dataset = dataframe_merge(dataset, train_dataset)
        #people = people_training.merge(people_labelling, on=config_db["id_column"], how='left')
        #train_dataset = train_dataset.append(dataset)

        feature_col_dict = merge_feature_dictionaries(feature_col_dict, features)

    test_label_date = fake_today_dt + datetime.timedelta(days = prediction_window)
    test_dataset, features_test = label_feature_producer(fake_today_dt,
                                                         test_label_date,
                                                         feature_list, labels)

    '''train_dataset = train_dataset.fillna(0)
    test_dataset = test_dataset.fillna(0)'''

    train_table_name = ('features_train_{}_{}_at_{}').format(fake_today,
        prediction_window, feature_timestamp)
    test_table_name = ('features_test_{}_{}_at_{}').format(fake_today,
        prediction_window, feature_timestamp)

    print 'writing training database'
    print train_dataset.shape
    #writer.write_dataframe_to_db(train_dataset, train_table_name, 'feature_tables')
    #train_dataset.to_sql(train_table_name, engine, if_exists='append', index=False, schema = 'feature_tables', chunksize= 500)
    write_dataframe_to_sql(train_table_name, train_dataset, 'feature_tables')

    print 'writing test database'
    #writer.write_dataframe_to_db(test_dataset, test_table_name, 'feature_tables')
    write_dataframe_to_sql(test_table_name, test_dataset, 'feature_tables')
    #test_dataset.to_sql(test_table_name, engine, if_exists='append', index= False, schema = 'feature_tables', chunksize = 1000)

    print 'writing json'
    json_file_name = ('{}classmap_{}_{}.json').format(config['class_map_dictionary_directory'],
                                                                                                            fake_today, prediction_window)
    with open(json_file_name, 'w') as fp:
        json.dump(feature_col_dict, fp)

    setup_environment.close_engine(engine)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

# http://stackoverflow.com/questions/31997859/
def write_dataframe_to_sql(df_name, df, schema):
    print('writing ', df_name)
    connection = engine.raw_connection()
    cursor = connection.cursor()

    # Hacky way to create the table with the necessary schema
    (df.iloc[[]]).to_sql(df_name, engine, schema = schema, index = False,
                         if_exists = 'replace')

    #stream the data using 'to_csv' and StringIO(); then use sql's 'copy_from' function
    output = cStringIO.StringIO()
    #ignore the index
    df.to_csv(output, sep = '\t', header = False, index = False)
    #jump to start of stream
    output.seek(0)
    skip = output.getvalue()
    cur = connection.cursor()
    #null values become ''
    cur.copy_from(output, schema + '."' + df_name + '"', null = "")
    connection.commit()
    cur.close()
