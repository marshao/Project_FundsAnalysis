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

    # features_data = pd.DataFrame(array_z, columns=feature_name)
    features_data = array_z
    return features_data, feature_name


def getLabels(labels):
    labels = pd.DataFrame(labels)
    # labels = np.array(labels)
    labels = labels.idxmax(axis=1)
    labels = np.array(labels.values)
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


def lstm_model_fn(features, labels, mode):
    # Tensor esitmator wants the paramers must be:
    # features, labels, mode, + or not params
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

    time_step = cell_numbers = 5
    hidden_units = 512  # hidden unit size
    classes = 3
    n_input = input_vector_size = 79  # input vector size
    drop_out = 0.4
    lr = 0.0001

    X_in = tf.reshape(features["x"], [-1, time_step, n_input])

    lstm_cell_1 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_2 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_3 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_4 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_5 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_6 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_7 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_8 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_9 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)
    lstm_cell_10 = tf.contrib.rnn.BasicLSTMCell(hidden_units, forget_bias=1.0, state_is_tuple=True)

    multi_lstm_cell = tf.nn.rnn_cell.MultiRNNCell([lstm_cell_1, lstm_cell_2, lstm_cell_3, lstm_cell_4, lstm_cell_5,
                                                   lstm_cell_6, lstm_cell_7, lstm_cell_8, lstm_cell_9, lstm_cell_10])

    outputs, last_state = tf.nn.dynamic_rnn(cell=multi_lstm_cell, inputs=X_in, dtype=tf.float64)
    # outputs, last_state = tf.nn.dynamic_rnn(cell=lstm_cell_1, inputs=X_in, dtype=tf.float64)
    # shape: outputs:[None, 5, 256]t
    # shape: last_state:[None, 256]
    # print last_state
    print last_state[9][1]

    dnn_1 = tf.layers.dense(inputs=last_state[9][1], units=1024, activation=tf.nn.relu, name='dnn_1')
    # shape:[None, 1024]

    drop_out_l = tf.layers.dropout(inputs=dnn_1, rate=drop_out, training=mode == tf.estimator.ModeKeys.TRAIN)

    logits = tf.layers.dense(inputs=drop_out_l, units=classes, name='output')

    ### Prediction Mode
    predictions = {
        'classes': tf.argmax(input=logits, axis=1, name='classes'),
        'probabilities': tf.nn.softmax(logits, name='Softmax_probabilities'),
        'logits': logits,
        'dnn_1': dnn_1,
        'drop_out_l': drop_out_l,
    }

    # eval_metric_ops = {
    #    'accuracy': tf.metrics.accuracy(labels=labels, predictions=predictions['classes'], name='system_accuracy')}

    if mode == tf.estimator.ModeKeys.PREDICT:
        return tf.estimator.EstimatorSpec(mode=mode, predictions=predictions)

    ###Training
    loss = tf.losses.sparse_softmax_cross_entropy(labels=labels, logits=logits)
    if mode == tf.estimator.ModeKeys.TRAIN:
        optimizer = tf.train.AdamOptimizer(learning_rate=lr)
        train_op = optimizer.minimize(loss=loss, global_step=tf.train.get_global_step())
        return tf.estimator.EstimatorSpec(mode=mode, loss=loss, train_op=train_op)

    ### Evaluation Branch
    eval_metric_ops = {
        'accuracy': tf.metrics.accuracy(labels=labels, predictions=predictions['classes'], name='system_accuracy')}
    # 'probabilities': predictions['probabilities']}
    if mode == tf.estimator.ModeKeys.EVAL:
        return tf.estimator.EstimatorSpec(mode=mode, loss=loss, eval_metric_ops=eval_metric_ops)


def main(argv):
    args = parser.parse_args(argv[1:])

    beg_date = '2015-01-01'
    # funds = ['002001_Nav']
    funds = ['240020_Nav']
    train_steps = 1700
    df_filtered = fund_Analysis(beg_date, funds)

    train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered, 'Week')
    # print train_sets.keys()
    # print train_sets['sample_sets'][0]
    # '''
    test_features_data, features_name, test_labels = getTFDataSets(test_sets)
    train_features_data, _, train_labels = getTFDataSets(train_sets)
    cv_features_data, _, cv_labels = getTFDataSets(cv_sets)

    train_input_fn = tf.estimator.inputs.numpy_input_fn(x={"x": train_features_data},
                                                        y=train_labels,
                                                        batch_size=50,
                                                        num_epochs=None,
                                                        shuffle=False
                                                        )

    eval_input_fn = tf.estimator.inputs.numpy_input_fn(x={"x": cv_features_data},
                                                       y=cv_labels,
                                                       shuffle=False
                                                       )
    pred_input_fn = tf.estimator.inputs.numpy_input_fn(x={"x": test_features_data},
                                                       shuffle=False
                                                       )

    # Define Esitmaters
    feature_cols = [tf.feature_column.numeric_column(k) for k in features_name]

    # tensors_to_log = {'probabiliteis': 'Softmax_probabilities'}
    # tensors_to_log = {'accuracy': 'system_accuracy'}
    # logging_hook = tf.train.LoggingTensorHook(tensors=tensors_to_log, every_n_iter=50)

    classifier = tf.estimator.Estimator(model_fn=lstm_model_fn,
                                        model_dir="/home/admini/PycharmProjects/Project_FundsAnalysis/LSTM_MultiCells")

    # train_op = classifier.train(input_fn=train_input_fn, max_steps=train_steps, hooks=[logging_hook])
    with tf.device('/gpu:0'):
        train_op = classifier.train(input_fn=train_input_fn, max_steps=train_steps)
    # print train_op
    with tf.device('/gpu:1'):
        eval_results = classifier.evaluate(input_fn=eval_input_fn, checkpoint_path=None)
        print eval_results
        # prediction_results = classifier.predict(input_fn=pred_input_fn, checkpoint_path=None)
        prediction_results = list(classifier.predict(input_fn=pred_input_fn, checkpoint_path=None))

        for each_result in prediction_results:
            print each_result['probabilities'], each_result['classes']


if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    tf.app.run(main)
