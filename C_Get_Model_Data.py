# -*- coding:utf-8 -*-
# !/usr/local/bin/python

# from __future__ import print_function


import numpy as np
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--batch_size', default=128, type=int)
parser.add_argument('--train_steps', default=500, type=int)


def getFeatures(samples, period):
    col_len = 82 * period
    array_z = np.zeros((1, col_len), dtype=np.float32)
    feature_name = samples[0].columns.tolist()
    feature_name = feature_name * period
    for i in range(0, len(feature_name)):
        feature_name[i] = '{}_{}'.format(feature_name[i], (i + 1))

    for sample in samples:
        row, col = sample.shape
        columns = sample.columns
        em_rows = period - row
        if em_rows > 0:
            df = pd.DataFrame(np.zeros((em_rows, col)), columns=columns)
            sample = pd.concat([sample, df])
        if em_rows < 0:
            sample = sample.iloc[1:, :]

        if array_z[0, 0] == 0:
            array = np.array(sample.values)
            array_z = np.reshape(array, (1, -1))
        else:
            array = np.array(sample.values)
            array_z = np.vstack((array_z, np.reshape(array, (1, -1))))

    # features_data = pd.DataFrame(array_z, columns=feature_name)
    features_data = array_z
    return features_data, feature_name


def getLabels(labels, tf=False):
    labels = pd.DataFrame(labels)
    # labels = np.array(labels)
    if tf:
        return np.array(labels)
    labels = labels.idxmax(axis=1)
    labels = np.array(labels.values)
    # print labels
    return labels


def getTFDataSets(each_set, period):
    samples = each_set['sample_sets']
    labels = each_set['label_sets']

    features_data, features_name = getFeatures(samples, period)
    labels = getLabels(labels)

    return features_data, features_name, labels
