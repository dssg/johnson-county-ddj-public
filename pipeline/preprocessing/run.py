import logging
import yaml
import datetime
import argparse
from ..queries import database_checker
from . import feature_table_builder
import pdb
import string


def main(config_file_name):
    logging.basicConfig(format = "%(asctime)s %(message)s",
            filename ="preprocessing.log", level = logging.DEBUG)
    #logger =logging.getLogger('Joco')


    logging.debug("test")
    logging.info("test")

    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
            logging.info("config file loaded")
    except:
        logging.exception("could not load config file")

    logging.debug(config.keys())

    feature_timestamp = str(datetime.datetime.now()).replace(' ',
        '_').translate(string.maketrans(string.punctuation,
                                        '_'*len(string.punctuation)))

    for fake_today in config['fake_today']:
        for prediction_window in config['prediction_window']:
            if database_checker.tables_exist(fake_today, str(prediction_window),
                feature_timestamp) == False:
                logging.debug("going to call feature generator")
                feature_table_builder.generate_feature_table(config, fake_today,
                    prediction_window, config['labeling_start_date'], feature_timestamp)
                logging.debug("database features for faketoday do not exist")
                
            else:
                logging.debug("features do exist for fake today")

    return(feature_timestamp)


if __name__ == "__main__":
    """ parser reads in the path to config yaml file
    with is passed in on the cmd line.  It passes
    this path to the main method above
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("config", type= str, help="pass your config",
                                                    default="default.yaml")
    args = parser.parse_args()
    main(args.config)
