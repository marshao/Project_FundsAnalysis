# -*- coding:utf-8 -*-
# !/usr/local/bin/python


from sklearn.datasets import load_iris
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.tree import DecisionTreeClassifier

iris = load_iris()

x = iris.data

y = iris.target

knn = KNeighborsClassifier(n_neighbors=3)

knn.fit(x, y)

test = [[3, 5, 4, 2], [5, 4, 3, 2]]
print knn.predict(test)

logreg = LogisticRegression()
logreg.fit(x, y)
print logreg.predict(test)

linreg = LinearRegression()
linreg.fit(x, y)
print linreg.predict(test)
