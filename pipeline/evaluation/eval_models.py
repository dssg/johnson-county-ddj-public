import pandas as pd
from .. import setup_environment
from sklearn import metrics
from .. import setup_environment
import pandas as pd

engine, config_db = setup_environment.get_database('pipeline/default_profile.yaml')
try:
    con = engine.raw_connection()
    # missing schema information ..
except:
    # change to log statement
    print 'cannot connect to database'

class ModelEvaluator():

    def __init__(self, model_id, threshold_pct, threshold_abs):
        self.model_id = model_id
        self.threshold_pct = threshold_pct
        self.threshold_abs = threshold_abs

    def generate_metrics(self):
        """Given a model id and a set of thresholds, obtain the y values (true
        class and predicted probability) and calculate metrics for the
        model at each threshold.

        :param batch_timestamp: timestamps of model batches
        :type batch_timestamp: list
        :returns: None -- always returns None as default
        :rtype: None
        """
        # get the y-values
        y_values = self.get_y_values()

        # generate metrics at thresholds
        eval_metrics_pct = self.threshold_pct.apply(self.evaluate_model_at_threshold,
                                               args = (y_values['scores'],
                                                       y_values['y_true'],
                                                       True))
        eval_metrics_abs = self.threshold_abs.apply(self.evaluate_model_at_threshold,
                                               args = (y_values['scores'],
                                                       y_values['y_true'],
                                                       False))

        # build table of metrics
        eval_metrics = pd.concat([eval_metrics_pct, eval_metrics_abs])
        eval_metrics_long = pd.melt(eval_metrics, id_vars = ['parameter'],
                                    var_name = 'metric')
        eval_metrics_long['unique_timestamp'] = self.model_id
        auc = self.compute_AUC(y_values['y_true'], y_values['scores'])
        final_metrics = eval_metrics_long.append({'parameter': 'roc',
            'metric': 'auc',
            'value': auc,
            'unique_timestamp': self.model_id},
            ignore_index = True)
        metrics_cols = ['parameter', 'metric', 'value', 'unique_timestamp']
        final_metrics = final_metrics[metrics_cols]

        return(final_metrics)


    def get_y_values(self):
        """ Return a dataframe containing the true classes and predicted
        probabilities for each case, sorted by descending probability.

        :returns: dataframe of true classes and predicted probabilities
        :rtype: pandas DataFrame
        """
        y_query = """
                SELECT
                    label as y_true,
                    prediction_prob as scores
                FROM
                    output.predictions
                WHERE
                    unique_timestamp = '{0}';
            """.format(self.model_id)
        y_values = pd.read_sql(y_query, con).sort_values(by = 'scores',
                                                         ascending = False)

        return y_values


    def evaluate_model_at_threshold(self, threshold_level, scores, y_true,
                                    percent):
        """ Given scores from a model, true classes for each individual in the
        training set, and a threshold level, return evaluation metrics for the
        model at each threshold.

        :param threshold_level: the level at which to classify a case as 1
        :param scores: predicted probability of class 1 for each case
        :param y_true: true class for each case
        :param percent: whether the threshold is a percent (True) or an absolute
                        number (False)
        :type threshold_level: int
        :type scores: pandas Series
        :type y_true: pandas Series
        :type percent: bool
        :returns: accuracy, precision, recall, false positive rate, and f1
        :rtype: pandas Series
        """
        depth = threshold_level
        if percent == True:
            percent_size = scores.size / 100
            depth = threshold_level * percent_size

        suffix = 'abs'
        if percent:
            suffix = 'pct'

        eval_metrics = pd.Series(index = ['parameter', 'accuracy', 'precision',
            'recall', 'false_pos_rate', 'f1'])

        y_pred = pd.Series(index = scores.index)
        y_pred[0:depth] = 1
        y_pred[depth::] = 0

        eval_metrics['parameter'] = '{0}_{1}'.format(threshold_level, suffix)
        eval_metrics['accuracy'] = metrics.accuracy_score(y_true, y_pred)
        eval_metrics['precision'] = metrics.precision_score(y_true, y_pred)
        eval_metrics['recall'] = metrics.recall_score(y_true, y_pred)
        eval_metrics['false_pos_rate'] = self.calc_false_pos_rate(y_true,
                                                                  y_pred)
        eval_metrics['f1'] = metrics.f1_score(y_true, y_pred)

        return(eval_metrics)


    def calc_false_pos_rate(self, y_true, y_pred):
        """ Given a set of true and predicted classes and a depth, return false
        positive rate at that depth.

        :param depth: The number or precent of individuals to include
        :param y_true: True class for each individual
        :param y_pred: Predicted class for each individual
        :param scores: Predicted probability for each individual
        :param percent: Whether depth is a percent (True) or an absolute number
                        (False)
        :type depth: float
        :type y_true: pandas Series
        :type y_pred: pandas Series
        :type scores: pandas Series
        :type percent: bool
        :returns: false positive rate
        :rtype: float
        """
        y_pred.name = 'y_pred'
        df = pd.concat([y_true, y_pred], axis = 1)
        false_pos = df[(df['y_pred'] == 1) & (df['y_true'] == 0)].count()[1].astype(float)
        true_neg = df[(df['y_pred'] == 0) & (df['y_true'] == 0)].count()[1]

        fpr = false_pos / (false_pos + true_neg)
        return(fpr)


    def compute_AUC(self, y_true, y_pred):
        """ Given a set of true labels and a set of predicted probabilities,
        return the area under the receiver operating characteristics curve.

        :param y_true: True class labels
        :type y_true: pandas Series
        :param y_pred: Predicted probabilities for positive class
        :type y_pred: pandas Series
        :returns: area under ROC curve
        :rtype: float
        """
        fpr, tpr, thresholds = metrics.roc_curve(y_true, y_pred, pos_label = 1)

        return metrics.auc(fpr, tpr)
