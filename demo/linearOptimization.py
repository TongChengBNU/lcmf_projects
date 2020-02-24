import numpy as np
from scipy import optimize

# row vector, min_{x} c*x
c = -1 * np.array([4.847, 6.882, 8.051, 7.650, 7.340])

# np.vstack receives a tuple to do vertical merge
A_uq = np.vstack((np.eye(5), np.array([[-1] + [0] * 4, [0, 0, 1, 0, 0], [0.5, 8.8, 9.9, 5.6, 7.6], [0.5, 18.5, 19.4, 7.3, 24.4]])))
b_uq = np.array([0.32] * 5 + [-0.12, 0.5, 7.2, 12])

# Idiot: A_eq must be 2 dimensional
A_eq = np.array([1] * 5, [0] * 5)
b_eq = np.array([1, 0])

bound = ((0,None), (0,None), (0,None), (0,None), (0,None))

res = optimize.linprog(c, A_ub=A_uq, b_ub=b_uq, A_eq=A_eq, b_eq=b_eq, bounds=bound)