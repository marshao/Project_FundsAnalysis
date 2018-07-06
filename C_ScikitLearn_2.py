# -*- coding:utf-8 -*-
# !/usr/local/bin/python


from sklearn.datasets import load_iris
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.svm import SVC
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import learning_curve
from sklearn import metrics
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

from C_Fund_Analysis import fund_Analysis, fund_data_proprocessing
from C_Get_Model_Data import getTFDataSets
import numpy as np
import pandas as pd


def metrixReport(y_true, y_pre):
    labels = [0, 1, 2]
    target_names = ['up', 'stay', 'down']
    print metrics.classification_report(y_true, y_pre, labels, target_names)
    print metrics.confusion_matrix(y_true, y_pre, labels)
    print metrics.accuracy_score(y_true, y_pre, labels)
    print metrics.precision_score(y_true, y_pre, labels, average='micro')


def main():
    beg_date = '2004-01-01'
    funds = ['002001_Nav']
    period = 25
    df_filtered = fund_Analysis(beg_date, funds)
    train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered,
                                                             degroup='Roll', split_portion=0.15, period=period)
    test_features_data, features_name, test_labels = getTFDataSets(test_sets, period)
    train_features_data, _, train_labels = getTFDataSets(train_sets, period)
    cv_features_data, _, cv_labels = getTFDataSets(cv_sets, period)

    X = np.append(np.append(train_features_data, cv_features_data, axis=0), test_features_data, axis=0)
    X_2 = np.append(train_features_data, cv_features_data, axis=0)
    y = np.append(np.append(train_labels, cv_labels, axis=0), test_labels, axis=0)
    y_2 = np.append(train_labels, cv_labels, axis=0)

    print "Sample Size: {}".format(X_2.shape)
    print "Labels size: {}".format(y_2.shape)

    pca = PCA(n_components=200)

    pca_X_2 = pca.fit_transform(X_2)
    pca_test = pca.fit_transform(test_features_data)
    print "PCAed Sample Size: {}".format(pca_X_2.shape)

    knn = KNeighborsClassifier(n_neighbors=18)
    knn_scores = cross_val_score(knn, X, y, cv=5)
    print "\n Knn_Score:"
    print knn_scores
    print knn_scores.mean()

    print "\n KNN no PCA"
    knn.fit(X_2, y_2)
    pre = knn.predict(test_features_data)
    metrixReport(test_labels, pre)
    print "\n KNN after PCA"
    knn.fit(pca_X_2, y_2)
    pre = knn.predict(pca_test)
    metrixReport(test_labels, pre)

    knn_bag = BaggingClassifier(base_estimator=KNeighborsClassifier(n_neighbors=10), max_samples=0.7, max_features=0.7,
                                n_estimators=5)
    knn_bag_scores = cross_val_score(knn_bag, X, y, cv=5)
    print "\n Knn_bag_score"
    print knn_bag_scores
    print knn_bag_scores.mean()

    print "\n KNN bag no PCA"
    knn_bag.fit(X_2, y_2)
    pre = knn_bag.predict(test_features_data)
    metrixReport(test_labels, pre)
    print "\n KNN bag after PCA"
    knn_bag.fit(pca_X_2, y_2)
    pre = knn_bag.predict(pca_test)
    metrixReport(test_labels, pre)

    random_forest = RandomForestClassifier(max_depth=5, max_features=0.5, n_estimators=10)
    random_forest_score = cross_val_score(random_forest, X, y, cv=5)
    print "\n Random Forest Score:"
    print random_forest_score
    print random_forest_score.mean()

    print "\n RF no PCA"
    random_forest.fit(X_2, y_2)
    pre = random_forest.predict(test_features_data)
    metrixReport(test_labels, pre)
    print "\n RF after PCA"
    random_forest.fit(pca_X_2, y_2)
    pre = random_forest.predict(pca_test)
    metrixReport(test_labels, pre)

    '''
    
    ada_clf = AdaBoostClassifier(n_estimators=21)
    ada_score = cross_val_score(ada_clf, X, y, cv=5)
    print "\n AdaBoost Classifier Score:"
    print ada_score
    print ada_score.mean()
    ada_clf.fit(X_2, y_2)
    pre = ada_clf.predict(test_features_data)
    metrixReport(test_labels, pre)


    svc_clf = SVC(decision_function_shape='ovo', tol=1e-4)
    svc_score = cross_val_score(svc_clf, X, y, cv=5)
    print "\n SVM Classifier Score:"
    print svc_score
    print svc_score.mean()
    svc_clf.fit(X_2, y_2)
    svc_clf.predict(test_features_data)
    metrixReport(test_labels, pre)

    train_size = np.linspace(.1, 1.0, 10)
    train_sizes, train_scores, test_scores = learning_curve(knn_bag, X, y, cv=5,  # scoring='neg_mean_squared_error',
                                                            train_sizes=train_size)
    train_scores_mean = np.mean(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)

    plt.plot(train_size, train_scores_mean, 'o-', color='red', label='Training')
    plt.plot(train_size, test_scores_mean, 'o-', color='blue', label='Testing')
    plt.xlabel('# of size')
    plt.ylabel('Value of scores')
    plt.show()
    '''


if __name__ == '__main__':
    main()
