import numpy as numpy
import pandas as pd
import logging
import sys
import pickle
import boto3
import datetime
import argparse
import yaml
from .. import setup_environment
#import experiment, models
import json
from ..queries import database_checker
from . import feature_model_grabber


import logging


engine, config_db = setup_environment.get_database()

def main(s3_profile, config_file_name, feature_timestamp, discard_models):
    """Replaces template placeholder with values.

    :param config_file_name: path to config yaml file
    :type config_file_name: str

    :returns: None -- always returns None as default
    :rtype: None
    """

    logging.basicConfig(format = "%(asctime)s %(message)s",
                        filename ="modeling.log", level = logging.INFO)


    log = logging.getLogger("Joco_Main")

    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
            log.info("loaded config file")
    except:
        log.exception("could not load config file")



    labels = []
    for label_key in config['labelling'].keys():
        if config['labelling'][label_key]:
            labels.append(label_key)


    #all_experiments = experiment.generate_models_to_run(config)
    #log.info("# of experiments to run: {}".format(len(all_experiments)))

    for fake_today in config['fake_today']:
        for prediction_window in config['prediction_window']:
            if database_checker.tables_exist(fake_today, prediction_window,
                feature_timestamp):
                featModelGrabber = feature_model_grabber.FeatureModelGrabber(fake_today, 
                    prediction_window, config, feature_timestamp,
                    s3_profile, discard_models)
                featModelGrabber.run(labels)
            #print fake_today, prediction_window


if __name__ == "__main__":
    """ parser reads in the path to config yaml file
    with is passed in on the cmd line.  It passes
    this path to the main method above
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("s3_profile", type = str,
                        help = "pass config path for s3 pickle storage",
                        default = ("/mnt/data/criminal_justice/"
                                   "johnson_county_ddj/config/"
                                   "s3_profile.json"))
    parser.add_argument("config", type = str, help = "pass your model config",
                        default="yamls/default_sample.yaml")
    parser.add_argument("feature_timestamp", type = str,
                        help = ("pass a timestamp if you want to use a specific"
                                " feature table set or pass a wildcard (%) to "
                                "use the most recent feature tables."),
                        default = "%")
    parser.add_argument("discard_models", type = bool,
                        help = ("Should models be automatically discarded at "
                                "cleanup?"),
                        default = False, nargs = '?')
    args = parser.parse_args()
    main(args.s3_profile, args.config, args.feature_timestamp,
         args.discard_models)
