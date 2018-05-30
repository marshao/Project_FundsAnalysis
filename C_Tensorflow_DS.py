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
    feature_name = samples[0].columns.tolist()
    feature_name = feature_name * 5
    for i in range(0, len(feature_name)):
        feature_name[i] = '{}_{}'.format(feature_name[i], (i + 1))


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
    labels = labels.idxmax(axis=1)
    labels = np.array(labels.values)
    #print labels
    return labels


def getTFDataSets(each_set):
    samples = each_set['sample_sets']
    labels = each_set['label_sets']

    features_data, features_name = getFeatures(samples)
    labels = getLabels(labels)
    #print labels

    # print features_data.index, labels.index
    # return tf.data.Dataset.from_tensor_slices((features, labels)), features_name
    return features_data, features_name, labels


# tf_train,feature_name= getTFDataSets(train_sets)
# tf_cv, _ = getTFDataSets(cv_sets)
test_features_data, features_name, test_labels = getTFDataSets(test_sets)
train_features_data, _, train_labels = getTFDataSets(train_sets)
cv_features_data, _, cv_labels = getTFDataSets(cv_sets)

'''
fea_holder = tf.placeholder(tf.float64, test_features_data.shape)
la_holder = tf.placeholder(tf.int32, test_labels.shape)
dataset = tf.data.Dataset.from_tensor_slices((fea_holder, la_holder))
iterator = dataset.make_initializable_iterator()
next_element = iterator.get_next()
'''
# Define Esitmaters
#feature_cols = [tf.feature_column.numeric_column(k) for k in features_name]

feature_cols = [tf.feature_column.numeric_column('feature', shape=[1, 395])]

'''
classifier = tf.estimator.DNNClassifier(feature_columns=feature_cols,
                                        hidden_units=[1024, 128],
                                        optimizer=tf.train.AdamOptimizer(1e-4),
                                        n_classes=3,
                                        dropout=0.5,
                                        model_dir='/home/marshao/DataMiningProjects/Project_FundsAnalysis/'
                                        )
'''
classifier = tf.estimator.DNNClassifier(n_classes=3, feature_columns=feature_cols, hidden_units=[1024, 128],
                                        optimizer=tf.train.AdamOptimizer(1e-4), dropout=0.5,
                                        model_dir="/home/marshao/DataMiningProjects/Project_FundsAnalysis/")
'''
def train_input_fn(dataset, num_epochs=None, shuffle=False):
    return tf.estimator.inputs.pandas_input_fn(
        x= {"fea_holder":dataset[0]},
        y = dataset[1],
        num_epochs=num_epochs,
        batch_size=128,
        shuffle=shuffle
    )
'''


def train_input_fn(feature, label, num_epochs=None, shuffle=False):
    return tf.estimator.inputs.numpy_input_fn(
        x={"feature": feature},
        y=label,
        num_epochs=num_epochs,
        batch_size=128,
        shuffle=shuffle
    )


def cv_input_fn(feature, label, num_epochs=None, shuffle=False):
    return tf.estimator.inputs.numpy_input_fn(
        x={"feature": feature},
        y=label,
        num_epochs=1,
        shuffle=shuffle
    )


def test_input_fn(feature, label, num_epochs=None, shuffle=False):
    return tf.estimator.inputs.numpy_input_fn(
        x={"feature": feature},
        y=label,
        num_epochs=1,
        shuffle=shuffle
    )


# sess = tf.Session()
# sess.run(iterator.initializer, feed_dict={fea_holder: train_features_data, la_holder: train_labels})

classifier.train(input_fn=lambda: train_input_fn(train_features_data, train_labels), steps=5000)

accuracy_op = classifier.evaluate(input_fn=lambda: test_input_fn(test_features_data, test_labels))['accuracy']
print("\nTest Accuracy: {0:f}%\n".format(accuracy_op *100))
