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
import matplotlib.pyplot as plt

from C_Fund_Analysis import fund_Analysis, fund_data_proprocessing
import numpy as np
import pandas as pd


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
	# funds = ['240020_Nav']

	df_filtered = fund_Analysis(beg_date, funds)

	train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered,
															 degroup='Roll', split_portion=0.15)

	test_features_data, features_name, test_labels = getTFDataSets(test_sets)
	train_features_data, _, train_labels = getTFDataSets(train_sets)
	cv_features_data, _, cv_labels = getTFDataSets(cv_sets)

	X = np.append(np.append(train_features_data, cv_features_data, axis=0), test_features_data, axis=0)
	X_2 = np.append(train_features_data, cv_features_data, axis=0)
	y = np.append(np.append(train_labels, cv_labels, axis=0), test_labels, axis=0)
	y_2 = np.append(train_labels, cv_labels, axis=0)

	print "Sample Size: {}".format(X_2.shape)
	print "Labels size: {}".format(y_2.shape)

	knn = KNeighborsClassifier(n_neighbors=18)
	knn_scores = cross_val_score(knn, X, y, cv=5)
	print "\n Knn_Score:"
	print knn_scores
	print knn_scores.mean()

	knn.fit(X_2, y_2)
	pre = knn.predict(test_features_data)
	metrixReport(test_labels, pre)

	knn_bag = BaggingClassifier(base_estimator=KNeighborsClassifier(n_neighbors=10), max_samples=0.7, max_features=0.7,
								n_estimators=5)
	knn_bag_scores = cross_val_score(knn_bag, X, y, cv=5)
	print "\n Knn_bag_score"
	print knn_bag_scores
	print knn_bag_scores.mean()

	knn_bag.fit(X_2, y_2)
	pre = knn_bag.predict(test_features_data)
	metrixReport(test_labels, pre)

	random_forest = RandomForestClassifier(max_depth=5, max_features=0.5, n_estimators=10)
	random_forest_score = cross_val_score(random_forest, X, y, cv=5)
	print "\n Random Forest Score:"
	print random_forest_score
	print random_forest_score.mean()

	random_forest.fit(X_2, y_2)
	random_forest.predict(test_features_data)
	metrixReport(test_labels, pre)

	ada_clf = AdaBoostClassifier(n_estimators=21)
	ada_score = cross_val_score(ada_clf, X, y, cv=5)
	print "\n AdaBoost Classifier Score:"
	print ada_score
	print ada_score.mean()
	ada_clf.fit(X_2, y_2)
	pre = ada_clf.predict(test_features_data)
	metrixReport(test_labels, pre)

	'''
	ada_range = range(10, 100)
	ada_scores = []
	for n in ada_range:
		ada_score = cross_val_score(AdaBoostClassifier(n_estimators=n), X, y, cv=5)
		ada_scores.append(ada_score.mean())
	'''

	svc_clf = SVC(decision_function_shape='ovo', tol=1e-4)
	svc_score = cross_val_score(svc_clf, X, y, cv=5)
	print "\n SVM Classifier Score:"
	print svc_score
	print svc_score.mean()
	svc_clf.fit(X_2, y_2)
	svc_clf.predict(test_features_data)
	metrixReport(test_labels, pre)

	'''
	train_size = np.linspace(.1, 1.0, 10)
	train_sizes, train_scores, test_scores = learning_curve(svc_clf, X, y, cv=5,  # scoring='neg_mean_squared_error',
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
