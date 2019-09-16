from sklearn import neighbors
import numpy as np
import random


x_data = [[random.randint(1,100) for _ in range(3)] for _ in range(100)]
nbrs = neighbors.NearestNeighbors(n_neighbors=2, algorithm='ball_tree').fit(x_data)
distances, indices = nbrs.kneighbors(x_data)
print(distances)
print(indices)













