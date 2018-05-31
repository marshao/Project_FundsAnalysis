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
    feature_name = feature_name * 5
    for i in range(0, len(feature_name)):
        feature_name[i] = '{}_{}'.format(feature_name[i], (i + 1))

    features_data = pd.DataFrame()
    sample_list = []
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

        if array_z[0, 0] == 0:
            array = np.array(sample.values)
            array_z = np.reshape(array, (1, -1))
        else:
            array = np.array(sample.values)
            array_z = np.vstack((array_z, np.reshape(array, (1, -1))))

    features_data = pd.DataFrame(array_z, columns=feature_name)

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
    # print inputs
    dataset = tf.data.Dataset.from_tensor_slices(inputs)
    dataset = dataset.repeat().batch(batch_size)
    # print dataset
    return feature, label


def test_input_fn(feature, label, batch_size=128, shuffle=False):
    features = dict(feature)
    if label is None:
        inputs = features
    else:
        inputs = (features, label)
    # print type(inputs)
    dataset = tf.data.Dataset.from_tensor_slices(inputs).batch(batch_size)

    return feature, label


def lstm_model_fn(features, labels, mode):
    # time_step is fund time period
    '''
    Input_size = [-1, 5, 79]
    Model V1: Single Cell LSTM RNN
    Model V2:
    :param feature:
    :param label:
    :param mode:
    :param time_step:
    :param hidden_units:
    :param classes:
    :param lr:
    :param drop_out:
    :return:
    '''

    time_step = 5
    hidden_units = [128, 256]
    classes = 3
    n_input = 79
    drop_out = 0.4
    lr = 0.0001

    # feature = features[0]
    # label = features[1]

    X_in = tf.reshape(features, [-1, time_step, n_input])

    lstm_cell_1 = tf.contrib.rnn.BasicLSTMCell(hidden_units[0], forget_bias=1.0)

    lstm_cell_2 = tf.contrib.rnn.BasicLSTMCell(hidden_units[1], forget_bias=1.0)

    multi_lstm_cell = tf.nn.rnn_cell.MultiRNNCell([lstm_cell_1, lstm_cell_2])

    outputs, last_state = tf.nn.dynamic_rnn(cell=multi_lstm_cell, inputs=X_in, dtype=tf.float64)
    # shape: outputs:[None, 5, 256]
    # shape: last_state:[None, 256]
    print last_state

    dnn_1 = tf.layers.dense(inputs=last_state[1], units=1024, activation=tf.nn.relu, name='dnn_1')
    # shape:[None, 1024]

    drop_out_l = tf.layers.dropout(inputs=dnn_1, rate=drop_out, training=mode == tf.estimator.ModeKeys.TRAIN)

    logits = tf.layers.dense(inputs=drop_out_l, units=3, name='output')

    ### Prediction Mode
    predictions = {
        'classes': tf.argmax(input=logits, axis=1, name='classes'),
        'probabilities': tf.nn.softmax(logits, name='Softmax_probabilities'),
        'logits': logits,
        'dnn_1': dnn_1,
        'drop_out_l': drop_out_l

    }

    if mode == tf.estimator.ModeKeys.PREDICT:
        return tf.estimator.EstimatorSpec(mode=mode, predictions=predictions)

    ###Training
    loss = tf.losses.sparse_softmax_cross_entropy(labels=labels, logits=logits)
    if mode == tf.estimator.ModeKeys.TRAIN:
        optimizer = tf.train.AdamOptimizer(learning_rate=lr)
        train_op = optimizer.minimize(loss=loss, global_step=tf.train.get_global_step())
        return tf.estimator.EstimatorSpec(mode=mode, loss=loss, train_op=train_op)

    ### Evaluation Branch
    eval_metric_ops = {'accuracy': tf.metrics.accuracy(labels=labels, predictions=predictions),
                       'probabilities': predictions['prebabilities']}
    return tf.estimator.EstimatorSpec(mode, loss, eval_metric_ops)


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
    train_features_data, _, train_labels = getTFDataSets(train_sets)
    cv_features_data, _, cv_labels = getTFDataSets(cv_sets)

    # Define Esitmaters
    feature_cols = [tf.feature_column.numeric_column(k) for k in features_name]

    # classifier = tf.estimator.Estimator(model_fn=lambda dataset, mode: lstm_model_fn(dataset, mode), model_dir="/lstm_model")

    classifier = tf.estimator.Estimator(model_fn=lstm_model_fn)
    # params={'feature_columns':feature_cols},
    # model_dir="/lstm_model")

    train_op = classifier.train(input_fn=lambda: train_input_fn(train_features_data, train_labels))


if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    tf.app.run(main)
