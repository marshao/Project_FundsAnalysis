# -*- coding:utf-8 -*-
# !/usr/local/bin/python

# from __future__ import print_function

from C_Fund_Analysis import fund_Analysis, fund_data_proprocessing
import tensorflow as tf
import numpy as np
import pandas as pd

beg_date = '2015-01-01'
funds = ['002001_Nav']
df_filtered = fund_Analysis(beg_date, funds)
train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered)


def getFeatures(samples):
    array_z = np.zeros((1, 395), dtype=np.float32)
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

    return array_z


def getLabels(labels):
    labels = np.array(labels)
    return labels


def getTFDataSets(each_set):
    samples = each_set['sample_sets']
    labels = each_set['label_sets']

    features = getFeatures(samples)
    labels = getLabels(labels)
    # return tf.data.Dataset.from_tensor_slices((features, labels))
    return features, labels


# tf_train = getTFDataSets(train_sets)
# tf_cv = getTFDataSets(cv_sets)
features, labels = getTFDataSets(test_sets)

fea_holder = tf.placeholder(tf.float64, features.shape)
la_holder = tf.placeholder(labels.dtype, labels.shape)
dataset = tf.data.Dataset.from_tensor_slices((fea_holder, la_holder))
iterator = dataset.make_initializable_iterator()
next_element = iterator.get_next()

sess = tf.Session()

sess.run(iterator.initializer, feed_dict={fea_holder: features, la_holder: labels})
for i in range(10):
    value = next_element
    print value
