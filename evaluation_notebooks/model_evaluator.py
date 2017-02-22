
import pandas as pd
import numpy as np
import pdb
import decimal
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import re
from sklearn import metrics


class ModelEvaluator():

    def __init__(self, model_id, threshold, engine):
        # values passed when initializing class
        self.model_id = model_id
        self.threshold = threshold.split('_')[0]
        self.threshold_level = int(threshold.split('_')[0])
        self.threshold_type = threshold.split('_')[-1]
        self.engine = engine

        # pull in data from database
        self.metrics = self.get_output_data('metrics')
        self.model = self.get_output_data('models')
        self.predictions = self.get_output_data('predictions')
        self.features = self.get_features().set_index('dedupe_id')
        
        # transform data
        self.predictions['dedupe_id'] = self.predictions['dedupe_id'].astype(int)
        self.predictions = self.predictions.set_index('dedupe_id')
        self.predictions = self.predictions.sort_values('prediction_prob',
                                                        ascending = False)
        self.labels = self.translate_labels()
        self.y_pred = self.classify_y()
        self.y_pred.name = 'y_pred'
        self.features = self.features.join(self.y_pred)
        self.depth = self.threshold_level
        if self.threshold_type == 'pct':
            percent_size = len(self.predictions) / 100
            self.depth = self.threshold_level * percent_size
        self.num_pos_cases = sum(self.predictions['label'])
        self.num_cases = len(self.predictions['label'])
        self.num_true_pos = int(self.predictions.head(self.depth)['label'].sum())
    
    def get_output_data(self, table_name):
        """ Pull model data from the database.

        :returns: requested model data 
        :rtype: pandas DataFrame
        """
        query = """
            SELECT DISTINCT
                *
            FROM
                output.{0}
            WHERE
                unique_timestamp = '{1}';
        """.format(table_name, self.model_id)
        return(pd.read_sql(query, self.engine))

    def get_features(self):
        """ Pull features from the database.

        :returns: features
        :rtype: pandas DataFrame
        """
        query = """
            SELECT
                *
            FROM
                {0}
            WHERE
                {1} is not null;
        """.format(self.model['test_table_name'][0],
                   self.model['labelling'].astype(str)[0])
        return(pd.read_sql(query, self.engine))

    def translate_labels(self):
        """ Translate positive/negative label into a more comprehensible
        description.

        :returns: label descriptions
        :rtype: list
        """
        if self.model['labelling'][0].split('_')[-1] == 'jims':
            labels = ['not booked', 'booked']
        if self.model['labelling'][0].split('_')[-1] == 'ems':
            labels = ['never calls EMS', 'calls EMS']
        if self.model['labelling'][0].split('_')[-1] == 'mh':
            labels = ['no entry into mental health', 'enters mental health']
        return(labels)

    def classify_y(self):
        """ Return the predicted classes for each case in the model.

        :returns: predicted classes for each case
        :rtype: pandas Series
        """
        depth = self.threshold_level
        if self.threshold_type == 'pct':
            percent_size = len(self.predictions) / 100
            depth = threshold_level * percent_size

        indices = self.predictions['prediction_prob'].sort_values(ascending = False).index
        y_pred = pd.Series(index = indices)
        y_pred[0:depth] = 1
        y_pred[depth::] = 0
        return(y_pred)

    def plot_normalized_confusion_matrix_at_depth(self):
        """ Returns a normalized confusion matrix.

        :returns: normalized confusion matrix
        :rtype: matplotlib figure
        """
        cm = metrics.confusion_matrix(self.predictions['label'], self.y_pred)
        np.set_printoptions(precision = 2)
        fig = plt.figure()
        cm_normalized = cm.astype('float') / cm.sum(axis = 1)[:, np.newaxis]

        plt.imshow(cm_normalized, interpolation = 'nearest',
                   cmap = plt.cm.Blues)
        plt.title("Normalized Confusion Matrix")
        plt.colorbar()
        tick_marks = np.arange(len(self.labels))
        plt.xticks(tick_marks, self.labels, rotation = 45)
        plt.yticks(tick_marks, self.labels)
        plt.tight_layout()
        plt.ylabel('True label')
        plt.xlabel('Predicted label')
        return(fig)

    def subset_metrics(self, suffix, metric):
        """ Subset the metrics table to include only the specified metric at the
        with the specified threshold type ('pct' for percent or 'abs' for
        absolute). Return the parameter and value columns.
        
        :param suffix: type of threshold
        :type suffix: str
        :param metric: name of the metric
        :type metric: str
        :returns: values of the metric at the appropriate thresholds
        :rtype: pandas DataFrame
        """
        subset = self.metrics.loc[((self.metrics['parameter'].str.contains(suffix)) &
                                   (self.metrics['metric'] == metric)),
                                  ['parameter', 'value']]
        subset['parameter'] = subset['parameter'].str.split('_').str.get(0).astype(int) 
        subset = subset.sort_values(by = 'parameter')
        return(subset)

    def plot_precision_recall_n(self, suffix):
        """ Generate a figure showing precision and recall curves over
        thresholds of the specified type ('pct' for percent or 'abs' for
        absolute).

        :param suffix: type of threshold
        :type suffix: str
        :returns: precision-recall curves
        :rtype: matplotlib figure
        """
        # extract and format the metrics
        precisions = self.subset_metrics(suffix, 'precision')
        recalls = self.subset_metrics(suffix, 'recall')
        precision_curve = precisions['value'][1:]
        recall_curve = recalls['value'][1:]

        # format x-axis
        k_above_per_thresh = np.arange(.01, 1.01, .01)
        if suffix == 'abs':
            k_above_per_thresh = np.arange(25, 2525, 25)
        
        # set up plot
        plt.clf()
        fig, ax1 = plt.subplots()
        ax1.plot(k_above_per_thresh, precision_curve, "#000099")
        if suffix == 'pct':
            ax1.set_xlabel('proportion of population')
            plt.xlim([0.0, 1.0])
        else:
            ax1.set_xlabel('number of individuals flagged')
            plt.xlim([0, 5000])
        ax1.set_ylabel('precision', color="#000099")
        plt.ylim([0.0, 1.0])
        ax2 = ax1.twinx()
        ax2.plot(k_above_per_thresh, recall_curve, "#CC0000")
        ax2.set_ylabel('recall', color="#CC0000")
        plt.ylim([0.0, 1.0])

        # give it a title
        if suffix == 'pct':
            plt.title("Precision-recall for top x proportion")
        else:
            plt.title("Precision-recall for top x individuals")

    def plot_ROC(self):
        """ Plot the receiver operating characteristic curve

        :returns: ROC curve
        :rtype: matplotlib figure
        """
        fpr = self.subset_metrics('pct', 'false_pos_rate')['value']
        tpr = self.subset_metrics('pct', 'recall')['value']
        thresholds = np.arange(.01, 1.01, .01)
        with plt.style.context(('ggplot')):
            fig, ax = plt.subplots()
            ax.plot(fpr, tpr, "#000099", label='ROC curve')
            ax.plot([0, 1], [0, 1], 'k--')
            plt.xlim([0.0, 1.0])
            plt.ylim([0.0, 1.05])
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.legend(loc='lower right')
            plt.title('Receiver operating characteristic')
        return(fig)

    def collapse_categorical_feature(self, feature_name):
        """ Collapse dummy variables generated for for model building into a
        single categorical column for examining distributions.

        :param feature_name: name of the categorical feature to collapse
        :type feature_name: str
        :returns: column containing the category for each person
        :rtype: pandas Series
        """
        expression = '{}_is_'.format(feature_name)
        feature_bin = pd.DataFrame(index = self.features.index)
        feature_bin[feature_name] = 'missing'
        for col in self.features.columns:
            if expression in col:
                category = re.sub(expression, '', col)
                feature_bin[feature_name][self.features[self.features[col] == 1].index] = category
        return(feature_bin[feature_name])

    def get_distribution_by_class(self, feature_column, label_column, prop):
        """ Get the distribution of a categorical feature within each class and
        return as a table with a row per class and a column per level of the
        feature.

        :param feature_column: name of the feature column on which to get
                               distributions
        :type feature_column: str
        :param label_column: name of predicted or true class column
        :type label_column: str
        :returns: proportions of each feature category in each class
        :rtype: pandas DataFrame
        """
        distributions = self.features.pivot_table(
            columns = feature_column,
            index = label_column,
            values = 'age_years',
            aggfunc = len,
            fill_value = 0)

        if prop:
            distributions = distributions.apply(lambda r: r/r.sum(), axis=1)

        return(distributions)

    def plot_deviations(self, feature_column):
        """ Plots deviations from expected distributions of features within each
        predicted class.

        :param feature_column: name of the column on which to plot distributions
        :type feature_column: str
        :returns: heatmap of deviations
        :rtype: matplotlib figure
        """
        expected_proportions = self.get_distribution_by_class(
            feature_column, self.model['labelling'][0], True)

        observed_proportions = self.get_distribution_by_class(
            feature_column, 'y_pred', True)

        observed_values = self.get_distribution_by_class(
            feature_column, 'y_pred', False)

        proportion_deviation = ((observed_proportions - expected_proportions) / 
                                expected_proportions)

        deviation_plot = sns.heatmap(proportion_deviation, cmap = 'RdBu_r',
                                     vmin = -1, vmax = 1,
                                     annot = observed_values, fmt = 'g')

        deviation_plot.set(xlabel = feature_column, ylabel = 'predicted class',
                           yticklabels = reversed(self.labels))

        return(deviation_plot)

    def describe_outcomes(self, event_type):
        """ Print out statistics about the outcomes for individuals correctly
        flagged positive.

        :param event_type: the type of event to lookup
        :type event_type: str
        :returns: None
        :rtype: None
        """
        events = self.get_events(event_type,
            "begin_date >= '{0}' and".format(self.features['training_end_date'].unique()[0]),
            tuple(self.predictions.head(self.depth).index.values.astype(str).tolist()))
        first_events = pd.DataFrame(events.groupby(['dedupe_id'],
                                    sort = False)['begin_date'].min())
        first_events['days_until'] = (first_events['begin_date'] - 
            self.features['training_end_date'].unique()[0])
        risk_scores = self.predictions.loc[events.dedupe_id.values,
                                            'prediction_prob'].drop_duplicates()
        
        print("Total number of events: {}".format(len(events)))
        print(("Average number of events per person experiencing event: "
               "%.2f" % events.dedupe_id.value_counts().mean()))
        print(("Correlation between risk score and number of events "
               "experienced: %.2f" % pd.concat([risk_scores,
                                                events.dedupe_id.value_counts()],
                                                axis = 1).corr().iloc[0, 1]))
        print(("Correlation between risk score and days until first event"
               ": %.2f" % pd.concat([risk_scores,
                                     first_events['days_until'].astype(int)],
                                    axis = 1).corr().iloc[0, 1]))

        if event_type != 'ems':
            print(("Correlation between risk score and total length of events: "
                ": %.2f" % pd.concat([risk_scores,
                events.groupby(['dedupe_id'])['length_days'].sum().astype(int)],
                axis = 1).corr().iloc[0, 1]))
            print("Total days: {}".format(events.length_days.sum()))
            print(("Average days per event: "
                   "{}".format(events.length_days.sum()/len(events))))
            print(("Average days per person: "
                   "{}".format(events.length_days.sum()/len(events.dedupe_id.unique()))))

        print(" ")
        print("At time of first event, median days since last... :")
        print(("... release from jail: "
               "{}".format(self.calculate_median_time_since_last(first_events,
                           'booking'))))
        print(("... mental health appointment: "
               "{}".format(self.calculate_median_time_since_last(first_events,
                           'mh'))))
        print(("... ems call: "
               "{}".format(self.calculate_median_time_since_last(first_events,
                           'ems'))))
        print(("NOTE: Medians are calculated only for those who have "
               "experienced both events in the correct order."))

    def calculate_median_time_since_last(self, first_events, service):
        """ Given a table of outcome events and a service ('booking', 'mh', or
        'ems'), return the median time since last contact with the named service
        at the time of the first event.

        :param events: outcome events
        :type events: pandas DataFrame
        :param service: the service to examine last contact with
        :type service: str
        :returns: median time since last contact with service at first event
        :rtype: datetime
        """
        if service == 'mh':
            services = self.get_mh_appt_dates(tuple(first_events.index.tolist()))
            services['svc_date'] = pd.to_datetime(services['svc_date'])
        else:
            services = self.get_events(service, '',
                tuple(first_events.index.tolist())).set_index('dedupe_id')
            if service == 'ems':
                services.rename(columns = {"begin_date": "svc_date"},
                                inplace = True)
            else:
                services.rename(columns = {"end_date": "svc_date",
                                           "begin_date": "b_date"},
                                inplace = True)
        services_events = pd.merge(services, first_events, left_index = True,
                                   right_index = True)
        services_events['date_diff'] = (((services_events.begin_date -
                                          services_events.svc_date) /
                                         np.timedelta64(1, 'D'))).astype(int)
        services_events = services_events[services_events['date_diff'] >= 0]
        services_events['dedupe_id'] = services_events.index
        services_events = services_events[services_events['date_diff'] > 0]
        days_since_service = services_events.groupby(['dedupe_id'],
            sort = False)['date_diff'].min()
        return(days_since_service.median())

    def get_mh_appt_dates(self, people):
        """ Get the dates of mental health appointments for people labeled 
        correctly by the model.

        :param people: list of ids to search for
        :type people: tuple
        :returns: mental health appointment data
        :rtype: pandas DataFrame
        """
        mh_services_query = """
            select
                dedupe_id,
                svc_date
            from
                clean.jocomentalhealthservices
            where
                svc_date < '{0}' and
                dedupe_id in {1}
        """.format(self.features['labeling_end_date'].unique()[0], people)
        return(pd.read_sql(mh_services_query, self.engine).set_index('dedupe_id'))

    def get_events(self, event_type, earliest_date, people):
        """ Select data from the events table for the given event type ('ems',
        'mh', 'booking') for the time period ranging from the earliest_date to
        the end of the prediction window.

        :param event_type: the type of event to lookup
        :type event_type: str
        :param earliest_date: SQL expression limiting the query to events before
                              a given date (must end with ' and'. If a blank
                              string is passed, all events prior to the end of
                              the prediction window will be selected.
        :type earliest_date: str
        :param people: people to get events for
        :type people: tuple
        :returns: all events of the given type for every person in the model
        :rtype: pandas DataFrame
        """
        events_query = """
            select 
                *
            from
                clean.canonical_events
            where
                {0}
                begin_date < '{1}' and
                dedupe_id in {2} and
                event = '{3}'
        """.format(earliest_date,
                   self.features['labeling_end_date'].unique()[0],
                   people,
                   event_type)
        events = pd.read_sql(events_query, self.engine)

        # for people still in jail or mental health, impute end date as last
        # observed end date
        if event_type != 'ems':
            end_date_query = """
                select 
                    end_date
                from
                    clean.canonical_events
                where
                    event = '{0}' and
                    end_date is not null
                order by
                    end_date desc
                limit 1
            """.format(event_type)
            final_end_date = pd.read_sql(end_date_query,
                                         self.engine)['end_date'][0]
            events.end_date.fillna(final_end_date, inplace = True)
            events['length_days'] = events.end_date - events.begin_date

        return(events)
