# -*- coding:utf-8 -*-
# !/usr/local/bin/python


import matplotlib.pyplot as plt

from C_Fund_Analysis import fund_Analysis, fund_data_proprocessing
from C_Get_Model_Data import getTFDataSets
from statsmodels.multivariate.pca import PCA
import numpy as np
import pandas as pd


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

    pca = PCA(X, ncomp=200)
    print pca.factors.shape
    print pca.ic
    print pca.eigenvals


if __name__ == '__main__':
    main()
