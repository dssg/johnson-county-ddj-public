import logging
import pandas as pd
import pandas.io.sql as pdsql
import pdb

from .. import setup_environment

engine, config_db = setup_environment.get_database('pipeline/default_profile.yaml')
try:
    con = engine.raw_connection()
    # missing schema information ..
except:
    # change to log statement
    print 'cannot connect to database'

def tables_exist(fake_today,prediction_window,feature_timestamp):
    table_list = pd.read_sql('''
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = 'feature_tables' AND
            table_name LIKE 'features_train_{}_{}_at_{}'
        ORDER BY
            table_name desc
        LIMIT
            1;
    '''.format(fake_today, prediction_window, feature_timestamp), con)
    
    if len(table_list) > 0:
        return True
    else:
        return False
