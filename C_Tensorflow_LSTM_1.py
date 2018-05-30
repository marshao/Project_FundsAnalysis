# -*- coding:utf-8 -*-
# !/usr/local/bin/python

# from __future__ import print_function

from C_Fund_Analysis import fund_Analysis, fund_data_proprocessing
import tensorflow as tf
import numpy as np
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--batch_size', default=128, type=int)
parser.add_argument('--train_steps', default=500, type=int)


def getFeatures(samples):
    # I like to create a 3 Dimensions Sample Sets:
    # D0: Calendar Days in a week
    # D1: Features
    # D0 * D1 make up one sample
    # D2: Calendar Weeks for samples make up sample set

    array_z = np.zeros((1, 395), dtype=np.float32)
    feature_name = samples[0].columns.tolist()
    # feature_name = feature_name * 5
    for i in range(0, len(feature_name)):
        feature_name[i] = '{}_{}'.format(feature_name[i], (i + 1))

    sample_list = []
    keys = []
    i = 0
    # print len(samples)
    # df_lstm_samples =
    for sample in samples:
        row, col = sample.shape
        columns = sample.columns
        em_rows = 5 - row
        if em_rows > 0:
            df = pd.DataFrame(np.zeros((em_rows, col)), columns=columns)
            sample = pd.concat([sample, df])
        if em_rows < 0:
            sample = sample.iloc[1:, :]
        print sample.values.tolist()
        break
        sample_list.append(sample)
        keys.append('week{}'.format(i))
        i += 1

    features_data = pd.concat(sample_list)

    # features_data = pd.DataFrame(array_z, columns=feature_name)
    print features_data.iloc[:, :3]
    return features_data, feature_name


def getLabels(labels):
    labels = pd.DataFrame(labels)
    # labels = np.array(labels)
    labels = labels.idxmax(axis=1)
    # print labels
    return labels


def getTFDataSets(each_set):
    samples = each_set['sample_sets']
    labels = each_set['label_sets']

    features_data, features_name = getFeatures(samples)
    labels = getLabels(labels)
    # print labels
    # print dict(features_data)

    # print features_data.index, labels.index
    # return tf.data.Dataset.from_tensor_slices((features, labels)), features_name
    return features_data, features_name, labels


def train_input_fn(feature, label, batch_size=128, shuffle=False):
    inputs = (dict(feature), label)
    # print type(inputs)
    dataset = tf.data.Dataset.from_tensor_slices(inputs)
    dataset = dataset.repeat().batch(batch_size)
    # print dataset
    return dataset


def test_input_fn(feature, label, batch_size=128, shuffle=False):
    features = dict(feature)
    if label is None:
        inputs = features
    else:
        inputs = (features, label)
    # print type(inputs)
    dataset = tf.data.Dataset.from_tensor_slices(inputs).batch(batch_size)

    return dataset


def lstm_model_fn(feature, label, mode, time_step=None, hidden_units=None, lr=0.0001, drop_out=0.5):
    # time_step is fund time period
    if time_step is None:
        time_step = 5
    if hidden_units is None:
        hidden_units = 79


def main(argv):
    args = parser.parse_args(argv[1:])

    beg_date = '2015-01-01'
    funds = ['002001_Nav']
    learning_rate = 0.0001
    drop_out = 0.5
    train_steps = 5000
    df_filtered = fund_Analysis(beg_date, funds)

    train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered, 'Week')
    test_features_data, features_name, test_labels = getTFDataSets(test_sets)
    # train_features_data, _, train_labels = getTFDataSets(train_sets)
    # cv_features_data, _, cv_labels = getTFDataSets(cv_sets)

    # Define Esitmaters
    feature_cols = [tf.feature_column.numeric_column(k) for k in features_name]

    classifier = tf.estimator.DNNClassifier(n_classes=3, feature_columns=feature_cols, hidden_units=[1024, 512, 128],
                                            optimizer=tf.train.AdamOptimizer(learning_rate), dropout=drop_out
                                            )

    # train_op = classifier.train(input_fn=lambda:train_input_fn(train_features_data, train_labels), steps = train_steps)

    # accuracy_op = classifier.evaluate(input_fn=lambda: test_input_fn(test_features_data, test_labels))
    # accuracy_op = accuracy_op['accuracy']
    # print("\nTest Accuracy: {0:f}%\n".format(accuracy_op*100))


if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    tf.app.run(main)
