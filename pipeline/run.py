import argparse
from .preprocessing import run as preprocessing_run
from .modeling import run as modeling_run
from .evaluation import run as evaluation_run

def main(config_file_name, eval_config_file, discard_models):
    """ run preprocessing, modeling, and evaluation

    :type s3_connection_config_path: str
    :param config_file_name: name of database configuration file
    :type config_file_name: str
    :param discard_models: whether or not the models should be automatically
                           discarded by the cleaning script
    :type discard_models: bool
    """
    print 'running preprocessing'
    feature_timestamp = preprocessing_run.main(config_file_name)

    print 'running models'
    modeling_run.main(config_file_name, feature_timestamp, discard_models)

    print 'running evaluation'
    evaluation_run.main(eval_config_file)


if __name__ == "__main__":
    """ parser reads in the path to config yaml file
    which is passed in on the cmd line.  It passes
    this path to the main method above
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, help="pass your config",
                        default="yamls/default_sample.yaml")
    parser.add_argument("eval_config_file", type=str,
                        help="pass the path to the evaluation config file",
                        default='pipeline/evaluation/eval_profile.yaml', nargs='?')
    parser.add_argument("discard_models", type=bool,
                        help=("should the models be automatically discarded by"
                              " the cleaning script?"),
                        default=False, nargs='?')
    args = parser.parse_args()
    main(args.config, args.eval_config_file, args.discard_models)
