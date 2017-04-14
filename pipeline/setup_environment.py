import os
import yaml
from sqlalchemy import create_engine
import yaml
import json
import logging

#logging.basicConfig(format = "%(asctime)s %(message)s",
#               filename ="setup_env.log", level = logging.DEBUG)

def get_database(config_file_name='pipeline/default_profile.yaml'):
    try:
        logging.debug('going to try to load connection')
        engine = get_connection_from_profile(config_file_name)
    except IOError:
        #add log statement
        print('Setup-Environment: IOError')
        return None, 'fail'

    config = get_config_file(config_file_name)

    if config == None:
        return None, 'failed to load config file'

    return engine,config

def get_config_file(config_file_name = 'pipeline/default_profile.yaml'):
    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
            #add log statement
            return config
    except:
        # add log statement
        print('failed to load experiment config file')
        return None

def get_connection_from_profile(config_file_name = "pipeline/default_profile.yaml"):
    logging.debug("going to try to get values")
    with open(config_file_name, 'r') as f:
        vals = yaml.load(f)

    logging.debug("got to values")
    logging.debug(vals)

    if not ('db_connection_config_path' in vals.keys()):
        raise Exception('Bad config file: '+ config_file_name + ' does not contain db config path')

    with open(vals['db_connection_config_path']) as f:
        db_config = json.load(f)
        engine = create_engine('postgres://', connect_args=db_config)

    return engine

def close_engine(engine):
    engine.dispose()

def get_uri_string(config_file_name = "pipeline/default_profile.yaml"):
    vals = get_config_file(config_file_name)


    if not ('PGHOST' in vals.keys() and
                    'PGUSER' in vals.keys() and
                    'PGPASSWORD' in vals.keys() and
                    'PGDATABASE' in vals.keys() and
                    'PGPORT' in vals.keys()):
        raise Exception('Bad config file: '+ config_file_name)

    uri = ("postgresql://{}:{}@{}:{}/{}").format(vals['PGUSER'], vals['PGPASSWORD'], vals['PGHOST'],vals['PGPORT'], vals['PGDATABASE'])
    return uri
