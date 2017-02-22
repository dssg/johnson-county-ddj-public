from .features import class_map
from .. import setup_environment
import pandas as pd
import datetime

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


class FeatureGrabber():

    def __init__(self,  end_date, engine, config_db, con):
        self.end_date = end_date
        self.engine = engine
        self.config_db = config_db
        self.con = con


    def __read_feature_from_db(self, query, 
                                                            drop_duplicates = True):
        print query
        results = pd.read_sql(query, con=self.con)
        return results

    def getFeature(self, feature_to_load):
        kwargs = {'fake_today' : self.end_date,
                  'db_tables' : self.config_db}
        feature = class_map.lookup(feature_to_load, **kwargs)

        
        if type(feature.query) == str:
            results = self.__read_feature_from_db(feature.query,
                                    drop_duplicates= True)
            featurenames = feature.description

        if feature.type_of_features == "categorical":
            results, featurenames = convert_categorical(results)
        elif feature.type_of_features == "numerical":
            results, featurenames = numerical_column_clean(results)
        elif feature.type_of_features == "imputation zero":
            results, featurenames = imputation_zero(results)
        else:
            results, featurenames = feature_name_grabber(results)

        return results, featurenames

def feature_name_grabber(df):
    df.fillna(0)
    columns = df.columns.values.tolist()
    columns[:] = (value for value in columns if value != config_db['id_column'])
    return df, columns


def convert_categorical(df):
    onecol = df.columns[1]
    onecol_name = df.columns.values.tolist()[1]
    df[onecol] = df[onecol].str.lower()
    categories = pd.unique(df[onecol])


    categories = [x for x in categories if x is not None]

    try:
        categories.remove(' ')
    except:
        pass

    categories = [str(x) for x in categories]

    categories = list(set([str.lower(x).strip() for x in categories]))

    #replaces spaces in middle of word w underscores
    categories = list(set([x.replace(" ", '_') for x in categories]))

    featnames = []
    for i in range(len(categories)):
        if type(categories[i]) is str:
            newfeatstr = onecol_name+'_is_' + categories[i] 
            featnames.append(newfeatstr)
            df[newfeatstr] = (df[onecol] == categories[i])

    onecol_null = onecol_name + "_is_null"
    df[onecol_null] = pd.isnull(df[onecol])
    df[onecol_null] = df[onecol_null].astype(float)
    df = df.drop(onecol, axis=1)
    df[featnames] = df[featnames].astype(float)
    df = df.groupby(config_db['id_column'], sort = False, as_index=False)[featnames].max()
    return df, featnames

def numerical_column_clean(df):
    df.fillna(0)
    columns = df.columns.values.tolist()
    columns[:] = (value for value in columns if value != config_db['id_column'])
    return df, columns

def imputation_zero(df):
    onecol_name = df.columns.values.tolist()[1]
    null_column_name = onecol_name + '_is_null'

    df[null_column_name] = pd.isnull(df[onecol_name])
    df[null_column_name] = df[null_column_name].astype(float)
    df = df.fillna(0)
    columns = df.columns.values.tolist()
    columns[:] = (value for value in columns if value != config_db['id_column'])
    return df, columns




