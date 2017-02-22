import numpy as np
import pandas as pd
from .. import setup_environment
from sklearn import preprocessing, cross_validation, svm, metrics, tree, decomposition, svm 
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier 
from sklearn.linear_model import LogisticRegression, Perceptron, SGDClassifier, OrthogonalMatchingPursuit, RandomizedLogisticRegression 
from sklearn.neighbors.nearest_centroid import NearestCentroid 
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB 
from sklearn.tree import DecisionTreeClassifier 
from sklearn.neighbors import KNeighborsClassifier 

config_db = setup_environment.get_config_file('pipeline/default_profile.yaml')


class ConfigError():
    # probabily exception error to be filled in later
    pass

class Model():
    def __init__(self, model_name, model_params, label, training_data,
                 testing_data, cols_to_use, config):
        self.model_name = model_name
        self.model_params = model_params
        self.label = label
        self.training_data = training_data
        self.testing_data = testing_data
        self.cols_to_use= cols_to_use
        self.config= config

    def get_data(self, df, undersample = False):
        # print warnings for expected columns not found in data
        for col in self.cols_to_use:
            if col not in (list(df.columns.values)):
                print "column not in "
                print col
                df[col] = 0
        # partition class labels for undersampling
        positive_label_df = df[df[self.label] ==1 ]
        negative_label_df = df[(df[self.label] ==0) & (df[self.label] != None)]

        if undersample:
            len_for_negative_labels =  len(positive_label_df) * 9
            if len_for_negative_labels <= len(negative_label_df):
                negative_label_df = negative_label_df.sample(len_for_negative_labels)

        df = negative_label_df.append(positive_label_df)


        _x = df[self.cols_to_use]
        # CAUTION -- for testing
        _x = _x.fillna(0)

        _y = df[self.label]
        _ids = df[config_db['id_column']]
        return _x, _y, _ids

    def get_training_data(self):
        training_x, training_y, training_ids = self.get_data(self.training_data,
                                                             undersample= True)
        return training_x, training_y, training_ids

    def get_test_data(self):
        test_x, test_y, test_ids = self.get_data(self.testing_data)
        return test_x, test_y, test_ids

    def run(self):
        training_x, training_y, training_ids = self.get_training_data()
        test_x, test_y, test_ids = self.get_test_data()
        clf = self.define_model(self.model_name, self.model_params)
        clf.fit(training_x, training_y)
        res_predict = clf.predict(test_x)
        if (self.model_name == "SGDClassifier" and (clf.loss =="hinge" or clf.loss == "perceptron")) or self.model_name == "linear.SVC":
            res = list(clf.decision_function(test_x))
        else:
            res = list(clf.predict_proba(test_x)[:,1])
        #fp, fn, tp, tn = self.compute_confusion_matrix(res[:,0], test_y)
        result_dictionary = {'training_ids': training_ids, 
                             'predictions_test_y': list(res_predict),
                             'prob_prediction_test_y': res ,
                             'test_y': list(test_y),
                             'test_ids': list(test_ids),
                             'model_name': self.model_name,
                             'model_params': self.model_params,
                             'label': self.label,
                             'feature_columns_used': self.cols_to_use,
                             'config': self.config,
                             'feature_importance': self.get_feature_importance(clf, self.model_name),
                             'columned_used_for_feat_importance': list(training_x.columns.values)}
        return  result_dictionary, clf

    def define_model(self, model, parameters, n_cores = 0):
        clfs = {'RandomForestClassifier': RandomForestClassifier(n_estimators=50, n_jobs=7),
                'ExtraTreesClassifier': ExtraTreesClassifier(n_estimators=10, n_jobs=7, criterion='entropy'),
                'AdaBoostClassifier': AdaBoostClassifier(DecisionTreeClassifier(max_depth=1), algorithm="SAMME", n_estimators=200),
                'LogisticRegression': LogisticRegression(penalty='l1', C=1e5),
                'svm.SVC': svm.SVC(kernel='linear', probability=True, random_state=0),
                'GradientBoostingClassifier': GradientBoostingClassifier(learning_rate=0.05, subsample=0.5, max_depth=6, n_estimators=10),
                'GaussianNB': GaussianNB(),
                'DecisionTreeClassifier': DecisionTreeClassifier(),
                'SGDClassifier': SGDClassifier(loss="hinge", penalty="l2", n_jobs=7),
                'KNeighborsClassifier': KNeighborsClassifier(n_neighbors=3), 
                'linear.SVC': svm.LinearSVC() }

        if model not in clfs:
            raise ConfigError("Unsupported model {}".format(model))

        clf = clfs[model]
        clf.set_params(**parameters)
        return clf

    def get_feature_importance(self,clf, model_name ):
        clfs = {'RandomForestClassifier':'feature_importances',
                'ExtraTreesClassifier': 'feature_importances',
                'AdaBoostClassifier': 'feature_importances',
                'LogisticRegression': 'coef',
                'svm.SVC': 'coef',
                'GradientBoostingClassifier': 'feature_importances',
                'GaussianNB': None,
                'DecisionTreeClassifier': 'feature_importances',
                'SGDClassifier': 'coef',
                'KNeighborsClassifier': None,
                'linear.SVC': 'coef'}

        if clfs[model_name] == 'feature_importances':
            return  list(clf.feature_importances_)
        elif clfs[model_name] == 'coef':
            return  list(clf.coef_.tolist())
        else:
            return None

    def compute_confusion_matrix(self,predicted_labels, labels):
        false_positive = 0
        false_negative = 0
        true_positive = 0
        true_negative = 0
        for idx, p_label in enumerate(predicted_labels):
            if p_label == 1 and labels[idx] == 1:
                true_positive +=1
            elif p_label ==1 and labels[idx] == 0:
                false_positive +=1
            elif p_label == 0 and labels[idx] == 1:
                false_negative += 1
            else:
                true_negative += 1

        return false_positive, false_negative, true_positive, true_negative




