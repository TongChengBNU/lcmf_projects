from sklearn import svm

import numpy as np
import random

def SupportVectorClassifier():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    #reg = svm.SVC(gamma='scale')
    # gamma must be real?
    # kernel = 'rbf'
    reg = svm.SVC(gamma=0.5, kernel='linear')
    reg.fit(x_data, y_data)
    # reg is an object
    support_vectors =  reg.support_vectors_
    print("Support vectors are: %s" % str(support_vectors))
    return reg


def SupportVectorRegression():
    x_data = np.array([[random.randint(1,10) for _ in range(5)] for _ in range(5)])
    y_data = np.array([random.randint(10,20) for _ in range(5)])
    #reg = svm.SVC(gamma='scale')
    # gamma must be real?
    reg = svm.SVR()
    reg.fit(x_data, y_data)
    # reg is an object
    support_vectors =  reg.support_vectors_
    print("Support vectors are: %s" % str(support_vectors))
    return reg







