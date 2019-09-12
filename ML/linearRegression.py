from sklearn import linear_model


import random
import numpy as np


def simple_LinearRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    reg = linear_model.LinearRegression()
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def ridge_LinearRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    reg = linear_model.Ridge(alpha=0.5)
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def ridgeCV_LinearRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    # please note that parameter should be 'alphas' (plural)
    reg = linear_model.RidgeCV(alphas = np.logspace(-6, 6, 13))
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    alpha = reg.alpha_
    print("Local optimized alpha: %s" % str(alpha))
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def lasso_LinearRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    reg = linear_model.Lasso(alpha=0.5)
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def elasticNet_LinearRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    reg = linear_model.ElasticNet(alpha=0.5, l1_ratio=0.7)
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def BayesianRidge_LinearRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    reg = linear_model.BayesianRidge()
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def AutoRelevance_Regressionn():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    reg = linear_model.ARDRegression(compute_score=True)
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg


def StochasticGradientDescent_Regression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    #reg = linear_model.SGDClassifier(loss='hinge', penalty='12', max_iter=5)
    reg = linear_model.SGDClassifier(loss='hinge', max_iter=5)
    reg.fit(x_data, y_data)
    # reg is an object
    coeff =  reg.coef_
    intercept = reg.intercept_
    print("Coefficients: %s" % str(coeff))
    print("Intercept: %s" % str(intercept))
    return reg























