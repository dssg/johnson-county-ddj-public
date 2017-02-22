import numpy as np
import pandas as pd
import argparse
import yaml
import json
import os
import logging
from .. import setup_environment
from tqdm import tqdm
from sklearn import metrics
from . import eval_models
import pdb


engine, config_db = setup_environment.get_database('pipeline/default_profile.yaml')
try:
    con = engine.raw_connection()
    # missing schema information ..
except:
    # change to log statement
    print 'cannot connect to database'



def main(eval_config_file):
    """ Runs evaluation code to generate metrics for models in the models table,
    stash them in a csv, and upload them in bulk to the metrics table.

    :param config_file_name: path to evaluation configuration file
    :type config_file_name: str
    :returns: None -- always returns None as default
    :rtype: None
    """
    # Intialize logging
    logging.basicConfig(format = "%(asctime)s %(message)s",
                        filename ="evaluation.log", level = logging.INFO)
    log = logging.getLogger("Joco_Main")

    # Read in config information
    try:
        with open(eval_config_file, 'r') as f:
            config = yaml.load(f)
            log.info("loaded evalutation config file")
    except:
        log.exception("could not load evaluation config file")
    threshold_percents = pd.Series(np.arange(config['relative_threshold_begin'],
                                   config['relative_threshold_end'],
                                   config['relative_threshold_increment']))
    threshold_absolutes = pd.Series(np.arange(config['absolute_threshold_begin'],
                                    config['absolute_threshold_end'],
                                    config['absolute_threshold_increment']))

    # Which models have not already been evaluated?
    model_query = """
        SELECT DISTINCT
            unique_timestamp
        FROM
            output.models
        WHERE
            unique_timestamp NOT IN (
                SELECT DISTINCT
                    unique_timestamp
                FROM
                    output.metrics
            );
        """
    models = pd.read_sql(model_query, engine)

    print("Found {0} models to evaluate. Beginning!".format(len(models)))
    # Start evaluating!
    for index, row in tqdm(models.iterrows()):
        try:
            model_evaluator = eval_models.ModelEvaluator(row[0],
                                                         threshold_percents,
                                                         threshold_absolutes)
            metrics = model_evaluator.generate_metrics()
            with open('results/metrics.csv', 'a+') as f:
                metrics.to_csv(f, header = False, index = False)
        except:
            with open('results/metrics_skipped.csv', 'a+') as f:
                f.write("{}\n".format(row[0]))

    # Get those metrics into the database and cleanup!
    print("Finished evaluating! Uploading to database and cleaning up!")
    with open(config_db['db_connection_config_path']) as f:
        db_vals = json.load(f)
    for k,v in db_vals.items():
        os.environ['PG' + k.upper()] = v if v else ""
    os.system("""
                cat results/metrics.csv | psql -c "COPY 
                output.metrics FROM stdin with csv;"
              """.format(config_db['predictions']))
    os.remove('results/metrics.csv')


if __name__ == "__main__":
    """ parser reads in the config file, containing thresholds.  It passes
    this to the main method above
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("eval_config_file",
                        type= str, help="pass an evaluation config file",
                        default='pipeline/evaluation/eval_profile.yaml',
                        nargs='?')
    args = parser.parse_args()
    main(args.eval_config_file)
