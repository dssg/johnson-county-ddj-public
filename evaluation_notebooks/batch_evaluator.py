
import pandas as pd
import numpy as np
import pdb
import decimal
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns


class BatchEvaluator():

    def __init__(self, batch_id_begin, batch_id_end, label,
                 most_important_metrics, prediction_window, num_top_models,
                 num_top_features, threshold, engine):
        # values passed when initializing class
        self.batch_id_begin = batch_id_begin
        self.batch_id_end = batch_id_end
        self.label = label
        self.most_important_metrics = most_important_metrics
        self.prediction_window = prediction_window
        self.num_top_models = num_top_models
        self.num_top_features = num_top_features
        self.threshold_level = int(threshold.split('_')[0])
        self.threshold_type = threshold.split('_')[-1]
        self.engine = engine

        # pull in data from database
        self.models = self.get_model_information()
        self.metrics = self.get_metrics()
        self.baserates, self.population_sizes, self.labeled_counts = (
            self.get_baseline_information())
        self.metrics_wide = self.metrics.pivot(index = 'unique_timestamp',
                                               columns = 'p_metric',
                                               values = 'value')
        self.metrics_models = self.join_metrics_and_models()
        self.top_models, self.top_models_metrics = self.get_top_models()
        self.feature_importance = self.get_feature_importances()
        self.top_features = self.get_top_features()
        self.predictions = self.get_predictions()
        self.positives = self.get_positives()


    def get_metrics(self):
        """Pull requested evaluation metrics for each model meeting the criteria
        specified in __init__.

        :returns: evaluation metrics for all models in the batches
        :rtype: pandas.DataFrame
        """
        metrics_query = """
            SELECT DISTINCT
                metrics.metric || '_' || metrics.parameter as p_metric,
                metrics.value,
                metrics.unique_timestamp
            FROM
                output.metrics as metrics
            WHERE
                metric || '_' || parameter in {0} AND
                unique_timestamp in {1};
        """.format(tuple(self.most_important_metrics),
                   tuple(self.models.index.astype(str).values))
        return(pd.read_sql(metrics_query, self.engine))


    def get_predictions(self):
        """Pull predictions for each model meeting the criteria specified in
        __init__.

        :returns: predictions for all models in the batches
        :rtype: pandas.DataFrame
        """
        rank_transformation = ''
        if self.threshold_type == 'pct':
            rank_transformation = "/ count(*) over w * 100"

        predictions_query = """
            WITH ranked_predictions AS (
                SELECT
                    row_number() OVER w::NUMERIC {0} AS rank,
                    *
                FROM
                    output.predictions 
                WHERE
                    unique_timestamp IN {1}
                WINDOW w AS (
                    PARTITION BY
                        unique_timestamp
                    ORDER BY
                        prediction_prob desc
                    ROWS BETWEEN
                        UNBOUNDED PRECEDING AND
                        UNBOUNDED FOLLOWING
                )
            )
            SELECT
                *
            FROM
                ranked_predictions 
            WHERE
                ranked_predictions.rank <= {2};
        """.format(rank_transformation,
                   tuple(self.top_models.index.astype(str).values),
                   self.threshold_level)
        predictions = pd.read_sql(predictions_query, self.engine)
        return(predictions.set_index('unique_timestamp').join(
               self.top_models[['model_id', 'fake_today']]))


    def get_model_information(self):
        """ Return a DataFrame containing model information for models being
        evaluated

        :returns: model information for all models in the batches
        :rtype: pandas.DataFrame
        """
        # specify criteria for model sampling
        where_clause = """
            batch_timestamp >= '{0}' AND
            batch_timestamp <= '{1}' AND
            labelling = '{2}' AND
            prediction_window = {3} AND
            discard_model = false
        """.format(self.batch_id_begin, self.batch_id_end, self.label,
                   self.prediction_window)

        # build query to select appropriate model data
        model_query = """
            SELECT
                *
            FROM
                output.models
            WHERE
                {0} AND
                models.fake_today != (
                                      select
                                          max(fake_today)
                                      from
                                          output.models
                                      WHERE
                                          {0}
                                     );
        """.format(where_clause)

        models = pd.read_sql(model_query,
                             self.engine).set_index('unique_timestamp')
        models["model_specs"] = (models["model_name"] + 
                                 models["model_params"] + 
                                 models["columns_for_feat_importance"])
        models['fake_today'] = models['fake_today'].astype('category')
        models['model_id'] = pd.Categorical.from_array(models.model_specs).codes
        return(models)


    def join_metrics_and_models(self):
        """ Join the metrics and models data on unique_timestamp

        :returns: the joined metrics and models data
        :rtype: pandas.DataFrame
        """
        metrics_models = self.metrics.set_index(
                                                'unique_timestamp'
                                                ).join(self.models)
        return(metrics_models)


    def get_top_models(self):
        """ Get a DataFrame of the models with the best average rank across all
        of the metrics.

        :returns: metrics and model information for the top models
        :rtype: pandas.DataFrame
        """
        # get ranks
        ranks = []
        for metric in self.most_important_metrics:
            metrics_windows = (
                self.metrics_models[self.metrics_models["p_metric"] == 
                metric].pivot_table(index = ['model_id'],
                                    columns = 'fake_today',
                                    values = 'value'))
            metrics_windows['min_value'] = (
                metrics_windows.min(axis = 1, numeric_only = True))
            ranks.append(metrics_windows['min_value'].rank())
        metric_ranks = pd.concat(ranks, axis = 1)
        metric_ranks['avg_rank'] = metric_ranks.mean(axis = 1)
        
        # get best models and their metrics
        top_ids = metric_ranks.sort_values('avg_rank',
            ascending = False).head(self.num_top_models).index
        top_models = self.models[self.models['model_id'].isin(top_ids)]
        top_models_metrics = (
            self.metrics_models[self.metrics_models['model_id'].isin(top_ids)])

        return(top_models, top_models_metrics)


    def get_baseline_information(self):
        """ Get population sizes, numbers of individuals with positive labels,
        and proportion of individuals with positive labels (baserates) for each
        prediction window.

        :returns: baserates, population sizes, and labeled counts
        :rtype: lists
        """
        baserates = []
        population_sizes = []
        labeled_counts = []
        for index, row in self.models[['fake_today',
            'test_table_name']].drop_duplicates().iterrows():
            labels_query = """
                SELECT
                    dedupe_id,
                    {0}
                FROM
                    {1}
                WHERE
                    {0} IS NOT NULL AND
                    training_end_date = '{2}';
            """.format(self.label, row['test_table_name'], row['fake_today'])
            labels = pd.read_sql(labels_query, self.engine)
            baserates.append(labels[self.label].sum()/float(len(labels)))
            population_sizes.append(len(labels))
            labeled_counts.append(labels[self.label].sum())
        return(baserates, population_sizes, labeled_counts)


    def get_unique_timestamp(self, model_id_number):
        """ Given a selected model id, obtain the unique timestamp for that
        model in the withheld prediction window.

        :param model_id_number: the model id for the selected model
        :type model_id_number: int
        :returns: timestamp for the model built in the final prediction window
        :rtype: str
        """
        # get a timestamp for one of the models of this type
        unique_timestamp = str(self.models[self.models['model_id'] ==
                               model_id_number].index.tolist()[0])
        
        # set conditions for matching model type
        where_clause = """
            batch_timestamp = (SELECT batch_timestamp
                               FROM output.models
                               WHERE unique_timestamp = '{0}') AND
            labelling = (SELECT labelling
                         FROM output.models
                         WHERE unique_timestamp = '{0}') AND
            prediction_window = (SELECT prediction_window
                                 FROM output.models
                                 WHERE unique_timestamp = '{0}') AND
            discard_model = false AND
            model_name = (SELECT model_name
                          FROM output.models
                          WHERE unique_timestamp = '{0}') AND
            columns_for_feat_importance = (SELECT columns_for_feat_importance
                                           FROM output.models
                                           WHERE unique_timestamp = '{0}') AND
            model_params = (SELECT model_params
                            FROM output.models
                            WHERE unique_timestamp = '{0}')
        """.format(unique_timestamp)

        # build a query to get the last model of this type
        selected_model_query = """
            SELECT
                unique_timestamp
            FROM
                output.models
            WHERE
                {0} AND
                models.fake_today = (SELECT max(fake_today)
                                     FROM output.models
                                     WHERE {0});
        """.format(where_clause)

        # run the query and return the value as a string
        final_timestamp = pd.read_sql(selected_model_query, self.engine)
        return(str(final_timestamp['unique_timestamp'][0]))


    def get_feature_importances(self):
        """ Feature importances and their associated features for a model are
        stored in separate cells in the models table. Turn all of the feature
        importances into a single DataFrame with columns for model id,
        prediction window start date, feature, and importance.

        :returns: feature importances for all models
        :rtype: pandas.DataFrame
        """
        feature_importance = pd.DataFrame({'model_id' : [],
                                          'fake_today' : [],
                                          'feature' : [],
                                          'importance' : []})
        for i, row in self.top_models.iterrows():
            if row['feature_importance'] is not None:

                if row['model_name'] == 'LogisticRegression' or 'linear.SVC':
                    importance = row['feature_importance'][2:-2].split(',')
                else:    
                    importance = row['feature_importance'][1:-1].split(',')
                feature = row['columns_for_feat_importance'][1:-1].split(',')
                importance = pd.Series([float(f) for f in importance])
                feature = pd.Series([str(f) for f in feature])
                fake_today = pd.Series([row['fake_today']] * len(feature))
                model = pd.Series([row['model_id']] * len(feature))
                model_data = pd.DataFrame({'model_id' : model,
                                          'fake_today' : fake_today,
                                          'feature' : feature,
                                          'importance' : importance})
                feature_importance = feature_importance.append(model_data)

        return(feature_importance) 


    def get_top_features(self):
        """ Find the features that have the highest average ranks across the top
        models.

        :returns: names of highest ranked features
        :rtype: pandas.DataFrame
        """
        features = self.feature_importance.pivot_table(index = 'feature',
                                                       columns = ['model_id',
                                                                  'fake_today'],
                                                       values = 'importance')
        feature_ranks = features.rank(axis = 0)
        feature_ranks['mean_rank'] = feature_ranks.mean(axis = 1)
        feature_ranks = feature_ranks.sort_values('mean_rank',
                                                  ascending = False)
        return(feature_ranks['mean_rank'].head(self.num_top_features))


    def get_positives(self):
        """ Given scores from the top models, return the predicted classes for
        each case in each model.

        :returns: predicted classes for each case at each threshold
        :rtype: pandas DataFrame
        """
        depth = self.threshold_level

        positives = pd.DataFrame({'model_id' : [],
                                 'fake_today' : [],
                                 'rank' : [],
                                 'dedupe_id' : []})
        predictions_wide = self.predictions.pivot_table(index = 'dedupe_id',
                                                        columns = ['model_id',
                                                        'fake_today'],
                                                        values = 'prediction_prob')

        for column in predictions_wide.columns:
            if self.threshold_type == 'pct':
                percent_size = len(column) / 100
                depth = int(self.threshold_level * percent_size)
            labeled = predictions_wide[column].sort_values(
                      ascending = False).index[0:depth].values
            model = pd.Series(np.repeat([column[0]], depth))
            rank = pd.Series(np.arange(0, depth, 1))
            fake_today = pd.Series(np.repeat([column[1]], depth))
            model_data = pd.DataFrame({'model_id' : model,
                                       'fake_today' : fake_today,
                                       'rank' : rank,
                                       'dedupe_id' : labeled})
            positives = positives.append(model_data)


        return(positives)


    def compute_jaccard_matrix(self, prediction_matrix):
        """ Given a matrix of individuals classified as positive from different
        models, return a correlation-matrix-like matrix of jaccard similarities.

        :param prediction_matrix: lists of top X indiviudals with highest risk
                                  scores according to different models
        :type prediction_matrix: pandas DataFrame 
        :returns: jarracrd matrix
        :rtype: pandas DataFrame
        """

        jaccard_matrix = pd.DataFrame(index = prediction_matrix.columns.values,
                                      columns = prediction_matrix.columns.values)
        for col_a in prediction_matrix.columns:
            position = prediction_matrix.columns.get_loc(col_a)
            for col_b in prediction_matrix.ix[:,position:]:
                intersection_cardinality = len(set.intersection(*[set(prediction_matrix[col_a]),
                                               set(prediction_matrix[col_b])]))
                union_cardinality = len(set.union(*[set(prediction_matrix[col_a]),
                                        set(prediction_matrix[col_b])]))
                jaccard = intersection_cardinality/float(union_cardinality)
                jaccard_matrix.loc[col_a, col_b] = jaccard
                jaccard_matrix.loc[col_b, col_a] = jaccard

        return(jaccard_matrix.astype(float))


    def describe_models(self, model_list):
        """ Given a list of model IDs, print information about those models.
        
        :param model_list: list of model IDs
        :type model_list: list
        :returns: None
        :rtype: None
        """
        for model in model_list:
            print("Model ID: {}".format(model))
            print(self.models.loc[self.models['model_id'] == model, 'model_name'].drop_duplicates().values)
            print(self.models.loc[self.models['model_id'] == model, 'model_params'].drop_duplicates().values)
            print(" ")
